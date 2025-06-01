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
