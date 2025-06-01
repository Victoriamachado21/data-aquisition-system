#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <memory>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

#pragma pack(push, 1)
struct LogRecord {
    char sensor_id[32];
    std::time_t timestamp;
    double value;
};
#pragma pack(pop)

std::time_t string_to_time_t(const std::string& time_string) {
    std::tm tm = {};
    std::istringstream ss(time_string);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return std::mktime(&tm);
}

std::string time_t_to_string(std::time_t time) {
    std::tm* tm = std::localtime(&time);
    std::ostringstream ss;
    ss << std::put_time(tm, "%Y-%m-%dT%H:%M:%S");
    return ss.str();
}

std::unordered_map<std::string, std::mutex> file_mutexes;

void write_log_record(const std::string& sensor_id, const LogRecord& record) {
    std::lock_guard<std::mutex> lock(file_mutexes[sensor_id]);
    std::ofstream file(sensor_id + ".bin", std::ios::binary | std::ios::app);
    file.write(reinterpret_cast<const char*>(&record), sizeof(LogRecord));
    file.close();
}

std::string read_log_records(const std::string& sensor_id, int num_records) {
    std::lock_guard<std::mutex> lock(file_mutexes[sensor_id]);
    std::ifstream file(sensor_id + ".bin", std::ios::binary);
    if (!file) return "ERROR|INVALID_SENSOR_ID\r\n";

    file.seekg(0, std::ios::end);
    std::streamsize size = file.tellg();
    int total_records = size / sizeof(LogRecord);
    int records_to_read = std::min(num_records, total_records);

    std::vector<LogRecord> records(records_to_read);
    file.seekg(-records_to_read * static_cast<int>(sizeof(LogRecord)), std::ios::end);
    file.read(reinterpret_cast<char*>(records.data()), records_to_read * sizeof(LogRecord));
    file.close();

    std::ostringstream response;
    response << records_to_read;
    for (const auto& record : records) {
        response << ";" << time_t_to_string(record.timestamp) << "|" << record.value;
    }
    response << "\r\n";
    return response.str();
}

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket) : socket_(std::move(socket)) {}

    void start() {
        read();
    }

private:
    void read() {
        auto self(shared_from_this());
        boost::asio::async_read_until(socket_, buffer_, "\r\n",
            [this, self](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    std::istream is(&buffer_);
                    std::string line;
                    std::getline(is, line);
                    if (!line.empty() && line.back() == '\r') line.pop_back();

                    if (line.rfind("LOG|", 0) == 0) {
                        handle_log(line);
                    } else if (line.rfind("GET|", 0) == 0) {
                        handle_get(line);
                    } else {
                        write("ERROR|UNKNOWN_COMMAND\r\n");
                    }
                }
            });
    }
    void handle_log(const std::string& line) {
        auto parts = split(line, '|');
        if (parts.size() != 4) {
            write("ERROR|INVALID_FORMAT\r\n");
            return;
        }

        LogRecord record;
        std::string sensor_id = parts[1];
        std::strncpy(record.sensor_id, sensor_id.c_str(), sizeof(record.sensor_id));
        record.timestamp = string_to_time_t(parts[2]);
        record.value = std::stod(parts[3]);

        // AJUDA A VER O QUE ESTA RECEBENDO
        std::cout << "[LOG] Sensor: " << sensor_id << ", Timestamp: " << parts[2] << ", Value: " << record.value << std::endl;

        write_log_record(sensor_id, record);
        read();
    }

    void handle_get(const std::string& line) {
        auto parts = split(line, '|');
        if (parts.size() != 3) {
            write("ERROR|INVALID_FORMAT\r\n");
            return;
        }

        std::string sensor_id = parts[1];
        int n = std::stoi(parts[2]);

        std::string response = read_log_records(sensor_id, n);

        // AJUDA A VER O QUE ESTA RECEBENDO
        std::cout << "[GET] Sensor: " << sensor_id << ", N: " << n << "\n[RESPOSTA] " << response;

        write(response);
    }

    void write(const std::string& message) {
        auto self(shared_from_this());
        boost::asio::async_write(socket_, boost::asio::buffer(message),
            [this, self](boost::system::error_code ec, std::size_t) {
                if (!ec) {
                    read();
                }
            });
    }

    std::vector<std::string> split(const std::string& str, char delimiter) {
        std::vector<std::string> tokens;
        std::istringstream stream(str);
        std::string token;
        while (std::getline(stream, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;
    }

    tcp::socket socket_;
    boost::asio::streambuf buffer_;
};

class Server {
public:
    Server(boost::asio::io_context& io_context, short port)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
        accept();
    }

private:
    void accept() {
        acceptor_.async_accept(
            [this](boost::system::error_code ec, tcp::socket socket) {
                if (!ec) {
                    std::make_shared<Session>(std::move(socket))->start();
                }
                accept();
            });
    }

    tcp::acceptor acceptor_;
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Uso: ./server <porta>\n";
        return 1;
    }

    boost::asio::io_context io_context;
    Server server(io_context, std::atoi(argv[1]));
    std::cout << "[INFO] Servidor iniciado na porta " << argv[1] << std::endl;
    io_context.run();
    return 0;
}
