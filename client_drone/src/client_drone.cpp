#include <iostream>
#include <thread>
#include <memory>
#include <string>
#include <chrono>
#include <atomic>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class Client : public std::enable_shared_from_this<Client> {
public:
    Client(boost::asio::io_context& io_context)
        : socket_(io_context), connected_(false) {}

    void connect(const tcp::resolver::results_type& endpoints) {
        auto self(shared_from_this());
        boost::asio::async_connect(socket_, endpoints,
            [this, self](boost::system::error_code ec, tcp::endpoint) {
                if (!ec) {
                    connected_ = true;
                    std::cout << "[Клиент] Подключился к серверу.\n";
                } else {
                    std::cout << "[Клиент] Ошибка подключения: " << ec.message() << "\n";
                }
            });
    }

    void send_message(const std::string& message) {
        if (!connected_) {
            std::cout << "[Клиент] Ещё не подключён к серверу.\n";
            return;
        }

        auto msg = std::make_shared<std::string>(message);
        auto self(shared_from_this());
        boost::asio::async_write(socket_, boost::asio::buffer(*msg),
            [this, self, msg](boost::system::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    std::cout << "[Клиент] Сообщение отправлено.\n";
                    do_read();
                } else {
                    std::cout << "[Клиент] Ошибка отправки: " << ec.message() << "\n";
                }
            });
    }

    bool is_connected() const { return connected_; }

private:
    void do_read() {
        auto self(shared_from_this());
        socket_.async_read_some(boost::asio::buffer(data_, max_length),
            [this, self](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    std::cout << "[Клиент] Получено от сервера: "
                              << std::string(data_, length) << "\n";
                } else {
                    std::cout << "[Клиент] Ошибка приёма: " << ec.message() << "\n";
                }
            });
    }

    tcp::socket socket_;
    std::atomic<bool> connected_;
    enum { max_length = 1024 };
    char data_[max_length];
};

int main() {
    try {
        boost::asio::io_context io_context;

        auto work_guard = boost::asio::make_work_guard(io_context);

        tcp::resolver resolver(io_context);
        auto endpoints = resolver.resolve("127.0.0.1", "12345");

        auto client = std::make_shared<Client>(io_context);
        client->connect(endpoints);

        std::thread t([&io_context]() { io_context.run(); });

        std::this_thread::sleep_for(std::chrono::seconds(1));

        if (!client->is_connected()) {
            std::cout << "[Клиент] Не удалось подключиться. Сервер запущен?\n";
        }

        std::string message;
        while (true) {
            std::cout << "Введите координаты (например, 55.75,37.61): ";
            std::getline(std::cin, message);
            if (message.empty())
                break;

            client->send_message(message);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        work_guard.reset();
        io_context.stop();
        t.join();

    } catch (std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << "\n";
    }

    return 0;
}
