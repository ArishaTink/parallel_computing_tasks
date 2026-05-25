#include <iostream>
#include <string>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

namespace asio = boost::asio;
using asio::ip::tcp;
using namespace asio::experimental::awaitable_operators;

asio::awaitable<std::string> read_from(tcp::socket& sock, std::string name) {
    char data[1024];

    auto [ec, n] = co_await sock.async_read_some(
        asio::buffer(data),
        asio::as_tuple(asio::use_awaitable));

    if (ec == asio::error::eof) {
        co_return "[" + name + "] Соединение закрыто (eof)\n";
    }
    if (ec == asio::error::operation_aborted) {
        co_return "[" + name + "] Операция отменена\n";
    }
    if (ec) throw boost::system::system_error(ec);

    co_return "[" + name + "] " + std::string(data, n);
}

asio::awaitable<void> multiplexer(tcp::socket sock1, tcp::socket sock2) {
    std::cout << "[Мультиплексор] Запущен, читаю из двух источников...\n";

    try {
        for (;;) {
            auto result = co_await (
                read_from(sock1, "sock1") || read_from(sock2, "sock2")
            );

            std::visit([](auto& val) {
                std::cout << val;
            }, result);
        }
    } catch (std::exception& e) {
        std::cout << "[Мультиплексор] Завершён: " << e.what() << "\n";
    }
}

asio::awaitable<void> accept_two_clients(tcp::acceptor& acceptor) {
    std::cout << "[Сервер] Ожидание первого клиента...\n";
    tcp::socket sock1 = co_await acceptor.async_accept(asio::use_awaitable);
    std::cout << "[Сервер] Клиент 1 подключился: "
              << sock1.remote_endpoint().address().to_string() << "\n";

    std::cout << "[Сервер] Ожидание второго клиента...\n";
    tcp::socket sock2 = co_await acceptor.async_accept(asio::use_awaitable);
    std::cout << "[Сервер] Клиент 2 подключился: "
              << sock2.remote_endpoint().address().to_string() << "\n";

    co_await multiplexer(std::move(sock1), std::move(sock2));
}

int main() {
    try {
        asio::io_context io_context;

        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), 12346));
        std::cout << "============================================\n";
        std::cout << " Мультиплексирование двух источников данных\n";
        std::cout << " Порт: 12346\n";
        std::cout << "============================================\n";
        std::cout << "Подключите два клиента:\n";
        std::cout << "  Терминал 1: nc 127.0.0.1 12346\n";
        std::cout << "  Терминал 2: nc 127.0.0.1 12346\n\n";

        asio::co_spawn(io_context,
            accept_two_clients(acceptor),
            asio::detached);

        io_context.run();

    } catch (std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << "\n";
    }

    return 0;
}
