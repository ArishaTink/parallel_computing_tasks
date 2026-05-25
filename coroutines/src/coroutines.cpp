#include <iostream>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace asio = boost::asio;
using asio::ip::tcp;

asio::awaitable<void> echo_session(tcp::socket sock) {
    char data[1024];
    try {
        std::cout << "[Сервер] Новый клиент подключился: "
                  << sock.remote_endpoint().address().to_string()
                  << ":" << sock.remote_endpoint().port() << "\n";

        for (;;) {
            auto [ec, n] = co_await sock.async_read_some(
                asio::buffer(data),
                asio::as_tuple(asio::use_awaitable));

            if (ec == asio::error::eof) {
                std::cout << "[Сервер] Клиент отключился (eof).\n";
                break;
            }
            if (ec) throw boost::system::system_error(ec);

            std::cout << "[Сервер] Получено: " << std::string(data, n);

            co_await asio::async_write(sock,
                asio::buffer(data, n),
                asio::use_awaitable);

            std::cout << "[Сервер] Эхо отправлено.\n";
        }
    } catch (std::exception& e) {
        std::cout << "[Сервер] Ошибка сессии: " << e.what() << "\n";
    }
}

asio::awaitable<void> listener(tcp::acceptor acceptor) {
    std::cout << "[Сервер] Ожидание подключений на порту "
              << acceptor.local_endpoint().port() << "...\n";

    for (;;) {
        tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);

        asio::co_spawn(
            acceptor.get_executor(),
            echo_session(std::move(socket)),
            asio::detached);
    }
}

int main() {
    try {
        asio::io_context io_context;

        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), 12345));
        std::cout << "[Сервер] Эхо-сервер на корутинах запущен.\n";

        asio::co_spawn(io_context, listener(std::move(acceptor)), asio::detached);

        io_context.run();

    } catch (std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << "\n";
    }

    return 0;
}
