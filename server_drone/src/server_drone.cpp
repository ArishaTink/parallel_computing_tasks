#include <iostream>
#include <vector>
#include <thread>
#include <memory>
#include <string>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

int global_message_count = 0;

std::shared_ptr<boost::asio::strand<boost::asio::io_context::executor_type>> strand;

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket) : socket_(std::move(socket)) {}

    void start() {
        std::cout << "[Сервер] Новый беспилотник подключился: "
                  << socket_.remote_endpoint().address().to_string()
                  << ":" << socket_.remote_endpoint().port() << std::endl;
        do_read();
    }

private:
    void do_read() {
        auto self(shared_from_this());
        socket_.async_read_some(boost::asio::buffer(data_, max_length),
            boost::asio::bind_executor(*strand,
                [this, self](boost::system::error_code ec, std::size_t length) {
                    if (!ec) {
                        std::string msg(data_, length);
                        std::cout << "[Сервер] Получено сообщение: " << msg << std::endl;

                        ++global_message_count;
                        std::cout << "[Сервер] Общее количество сообщений: "
                                  << global_message_count << std::endl;

                        do_write(length);
                    } else {
                        std::cout << "[Сервер] Клиент отключился." << std::endl;
                    }
                }));
    }

    void do_write(std::size_t length) {
        auto self(shared_from_this());
        boost::asio::async_write(socket_, boost::asio::buffer(data_, length),
            boost::asio::bind_executor(*strand,
                [this, self](boost::system::error_code ec, std::size_t /*length*/) {
                    if (!ec) {
                        std::cout << "[Сервер] Эхо-ответ отправлен." << std::endl;
                        do_read();
                    }
                }));
    }

    tcp::socket socket_;
    enum { max_length = 1024 };
    char data_[max_length];
};

class Server {
public:
    Server(boost::asio::io_context& io_context, short port)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
        std::cout << "[Сервер] Запущен на порту " << port << std::endl;
        std::cout << "[Сервер] Ожидание подключений беспилотников..." << std::endl;
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept(
            boost::asio::make_strand(acceptor_.get_executor()),
            [this](boost::system::error_code ec, tcp::socket socket) {
                if (!ec) {
                    std::make_shared<Session>(std::move(socket))->start();
                }
                do_accept();
            });
    }

    tcp::acceptor acceptor_;
};

int main() {
    try {
        boost::asio::io_context io_context;

        strand = std::make_shared<boost::asio::strand<boost::asio::io_context::executor_type>>(
            io_context.get_executor());

        Server server(io_context, 12345);

        std::vector<std::thread> threads;
        const int num_threads = 4;

        std::cout << "[Сервер] Запуск " << num_threads << " потоков обработки." << std::endl;

        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&io_context]() {
                io_context.run();
            });
        }

        for (auto& t : threads) {
            t.join();
        }

    } catch (std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << std::endl;
    }

    return 0;
}
