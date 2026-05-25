#include <iostream>
#include <vector>
#include <thread>
#include <random>
#include <atomic>
#include <cstdint>
#include <stdexcept>
#include <boost/asio.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace asio = boost::asio;

class BankAccount {
public:
    BankAccount(asio::io_context& io_context)
        : strand_(asio::make_strand(io_context)), balance_(0) {}

    asio::awaitable<void> async_deposit(int64_t amount) {
        co_await asio::post(strand_, asio::use_awaitable);

        balance_ += amount;
    }

    asio::awaitable<void> async_withdraw(int64_t amount) {
        co_await asio::post(strand_, asio::use_awaitable);

        if (amount > balance_) {
            throw std::invalid_argument("Insufficient funds");
        }
        balance_ -= amount;
    }

    asio::awaitable<int64_t> async_get_balance() {
        co_await asio::post(strand_, asio::use_awaitable);

        co_return balance_;
    }

    int64_t get_balance_sync() const { return balance_; }

private:
    asio::strand<asio::io_context::executor_type> strand_;
    int64_t balance_;
};

asio::awaitable<void> run_transactions(
    BankAccount& account,
    int coroutine_id,
    std::atomic<int64_t>& total_deposited,
    std::atomic<int64_t>& total_withdrawn,
    std::atomic<int>& completed_count)
{
    std::mt19937 rng(coroutine_id * 42 + 7);
    std::uniform_int_distribution<int64_t> amount_dist(1, 100);

    for (int i = 0; i < 10; ++i) {
        int64_t amount = amount_dist(rng);
        co_await account.async_deposit(amount);
        total_deposited.fetch_add(amount);
    }

    for (int i = 0; i < 10; ++i) {
        int64_t amount = amount_dist(rng);
        try {
            co_await account.async_withdraw(amount);
            total_withdrawn.fetch_add(amount);
        } catch (const std::invalid_argument& e) {
        }
    }

    completed_count.fetch_add(1);
}

int main() {
    try {
        asio::io_context io_context;

        BankAccount account(io_context);

        const int NUM_COROUTINES = 100;
        const int NUM_THREADS = 4;

        std::atomic<int64_t> total_deposited{0};
        std::atomic<int64_t> total_withdrawn{0};
        std::atomic<int> completed_count{0};

        std::cout << "============================================\n";
        std::cout << " Банковский счёт: асинхронные транзакции\n";
        std::cout << " Корутин: " << NUM_COROUTINES << "\n";
        std::cout << " Потоков: " << NUM_THREADS << "\n";
        std::cout << "============================================\n\n";

        for (int i = 0; i < NUM_COROUTINES; ++i) {
            asio::co_spawn(io_context,
                run_transactions(account, i,
                    total_deposited, total_withdrawn, completed_count),
                asio::detached);
        }

        std::vector<std::thread> threads;
        for (int i = 0; i < NUM_THREADS; ++i) {
            threads.emplace_back([&io_context]() {
                io_context.run();
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        int64_t final_balance = account.get_balance_sync();
        int64_t expected_balance = total_deposited.load() - total_withdrawn.load();

        std::cout << "\n--- Результаты ---\n";
        std::cout << "  Завершено корутин:    " << completed_count.load() << "\n";
        std::cout << "  Всего депозитов:      " << total_deposited.load() << "\n";
        std::cout << "  Всего снятий:         " << total_withdrawn.load() << "\n";
        std::cout << "  Ожидаемый баланс:     " << expected_balance << "\n";
        std::cout << "  Фактический баланс:   " << final_balance << "\n";
        std::cout << "  Совпадение:           "
                  << (final_balance == expected_balance ? "ДА" : "НЕТ (гонка!)")
                  << "\n\n";

        if (final_balance == expected_balance) {
            std::cout << "Strand корректно защищает баланс от гонок данных.\n";
        } else {
            std::cout << "ОШИБКА: обнаружена гонка данных!\n";
        }

    } catch (std::exception& e) {
        std::cerr << "Ошибка: " << e.what() << "\n";
    }

    return 0;
}
