#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <random>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <execution>
#include <functional>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <barrier>
#include <cmath>

struct SensorData {
    int sensor_id;
    double value;
    double timestamp;
    int data_type;
};

struct AggregatedResult {
    int sensor_id;
    double mean;
    double min_val;
    double max_val;
    double sum;
    double stddev;
    int count;
    double prefix_sum;
};

template<typename T>
class ThreadSafeQueue {
private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stopped_{false};

public:
    void push(T item) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(std::move(item));
        }
        cv_.notify_one();
    }

    void push_batch(std::vector<T>& items) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& item : items) {
                queue_.push(std::move(item));
            }
        }
        cv_.notify_all();
    }

    bool pop(T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty() || stopped_.load(); });
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    bool try_pop(T& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void stop() {
        stopped_.store(true);
        cv_.notify_all();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
};

template<typename T>
class ConcurrentVector {
private:
    std::vector<T> data_;
    mutable std::shared_mutex mutex_;

public:
    void push_back(const T& item) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_.push_back(item);
    }

    std::vector<T> snapshot() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return data_;
    }

    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return data_.size();
    }

    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_.clear();
    }
};

class SensorInputManager {
private:
    int num_sensors_;
    int num_threads_;
    std::vector<std::thread> threads_;
    ThreadSafeQueue<SensorData>& output_queue_;
    std::atomic<bool> running_{true};
    std::atomic<int> total_generated_{0};

    void sensor_thread_func(int thread_id, int start_sensor, int end_sensor) {
        std::mt19937 rng(thread_id * 42 + 7);
        std::uniform_real_distribution<double> temp_dist(15.0, 120.0);
        std::uniform_real_distribution<double> pressure_dist(0.5, 15.0);
        std::uniform_real_distribution<double> vibration_dist(0.0, 50.0);

        std::cout << "[SensorInput] Поток " << thread_id
                  << " обслуживает сенсоры [" << start_sensor
                  << ", " << end_sensor << ")\n";

        int batch_count = 0;
        while (running_.load()) {
            std::vector<SensorData> batch;
            double ts = std::chrono::duration<double>(
                std::chrono::steady_clock::now().time_since_epoch()).count();

            for (int s = start_sensor; s < end_sensor; ++s) {
                SensorData d;
                d.sensor_id = s;
                d.timestamp = ts;
                d.data_type = s % 3;

                switch (d.data_type) {
                    case 0: d.value = temp_dist(rng); break;
                    case 1: d.value = pressure_dist(rng); break;
                    case 2: d.value = vibration_dist(rng); break;
                }
                batch.push_back(d);
            }

            output_queue_.push_batch(batch);
            total_generated_.fetch_add(batch.size());
            batch_count++;

            if (batch_count >= 10) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

public:
    SensorInputManager(int num_sensors, int num_threads, ThreadSafeQueue<SensorData>& queue)
        : num_sensors_(num_sensors), num_threads_(num_threads), output_queue_(queue) {}

    void start() {
        int sensors_per_thread = num_sensors_ / num_threads_;
        for (int i = 0; i < num_threads_; ++i) {
            int start = i * sensors_per_thread;
            int end = (i == num_threads_ - 1) ? num_sensors_ : start + sensors_per_thread;
            threads_.emplace_back(&SensorInputManager::sensor_thread_func, this, i, start, end);
        }
    }

    void stop() {
        running_.store(false);
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
        output_queue_.stop();
    }

    int total_generated() const { return total_generated_.load(); }
};

class Dispatcher {
private:
    ThreadSafeQueue<SensorData>& input_queue_;
    std::vector<ThreadSafeQueue<SensorData>*> worker_queues_;
    std::thread thread_;
    std::atomic<bool> running_{true};
    std::atomic<int> dispatched_{0};

    void dispatch_func() {
        std::cout << "[Dispatcher] Запущен, распределяю данные по "
                  << worker_queues_.size() << " обработчикам\n";

        while (running_.load()) {
            SensorData data;
            if (input_queue_.pop(data)) {
                int worker_idx = data.sensor_id % worker_queues_.size();
                worker_queues_[worker_idx]->push(data);
                dispatched_.fetch_add(1);
            }
        }
    }

public:
    Dispatcher(ThreadSafeQueue<SensorData>& input,
               std::vector<ThreadSafeQueue<SensorData>*>& workers)
        : input_queue_(input), worker_queues_(workers) {}

    void start() {
        thread_ = std::thread(&Dispatcher::dispatch_func, this);
    }

    void stop() {
        running_.store(false);
        input_queue_.stop();
        if (thread_.joinable()) thread_.join();
        for (auto* q : worker_queues_) q->stop();
    }

    int dispatched() const { return dispatched_.load(); }
};

class ProcessingPool {
private:
    int num_workers_;
    std::vector<ThreadSafeQueue<SensorData>> worker_queues_;
    std::vector<std::thread> threads_;
    ConcurrentVector<AggregatedResult>& results_;
    std::atomic<bool> running_{true};
    std::atomic<int> processed_{0};

    void worker_func(int worker_id) {
        std::cout << "[ProcessingPool] Обработчик " << worker_id << " запущен\n";

        std::unordered_map<int, std::vector<double>> local_buffer;

        while (running_.load()) {
            SensorData data;
            if (worker_queues_[worker_id].try_pop(data)) {
                local_buffer[data.sensor_id].push_back(data.value);
                processed_.fetch_add(1);

                for (auto it = local_buffer.begin(); it != local_buffer.end(); ) {
                    if (it->second.size() >= 10) {
                        auto& values = it->second;

                        double sum = std::transform_reduce(
                            std::execution::par_unseq,
                            values.begin(), values.end(),
                            0.0, std::plus<>(),
                            [](double v) { return v; }
                        );

                        double mean = sum / values.size();

                        double variance = std::transform_reduce(
                            std::execution::par_unseq,
                            values.begin(), values.end(),
                            0.0, std::plus<>(),
                            [mean](double v) { return (v - mean) * (v - mean); }
                        ) / values.size();

                        std::sort(std::execution::par, values.begin(), values.end());

                        std::vector<double> prefix_sums(values.size());
                        std::inclusive_scan(
                            std::execution::par_unseq,
                            values.begin(), values.end(),
                            prefix_sums.begin()
                        );

                        AggregatedResult result;
                        result.sensor_id = it->first;
                        result.mean = mean;
                        result.min_val = values.front();
                        result.max_val = values.back();
                        result.sum = sum;
                        result.stddev = std::sqrt(variance);
                        result.count = values.size();
                        result.prefix_sum = prefix_sums.back();

                        results_.push_back(result);

                        it = local_buffer.erase(it);
                    } else {
                        ++it;
                    }
                }
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }

        for (auto& [sensor_id, values] : local_buffer) {
            if (!values.empty()) {
                double sum = std::reduce(std::execution::par_unseq,
                                         values.begin(), values.end(), 0.0);
                double mean = sum / values.size();

                std::sort(std::execution::par, values.begin(), values.end());

                std::vector<double> prefix_sums(values.size());
                std::inclusive_scan(std::execution::par_unseq,
                                   values.begin(), values.end(),
                                   prefix_sums.begin());

                double variance = std::transform_reduce(
                    std::execution::par_unseq,
                    values.begin(), values.end(),
                    0.0, std::plus<>(),
                    [mean](double v) { return (v - mean) * (v - mean); }
                ) / values.size();

                AggregatedResult result;
                result.sensor_id = sensor_id;
                result.mean = mean;
                result.min_val = values.front();
                result.max_val = values.back();
                result.sum = sum;
                result.stddev = std::sqrt(variance);
                result.count = values.size();
                result.prefix_sum = prefix_sums.back();
                results_.push_back(result);
            }
        }
    }

public:
    ProcessingPool(int num_workers, ConcurrentVector<AggregatedResult>& results)
        : num_workers_(num_workers), worker_queues_(num_workers), results_(results) {}

    std::vector<ThreadSafeQueue<SensorData>*> get_queue_ptrs() {
        std::vector<ThreadSafeQueue<SensorData>*> ptrs;
        for (auto& q : worker_queues_) ptrs.push_back(&q);
        return ptrs;
    }

    void start() {
        for (int i = 0; i < num_workers_; ++i) {
            threads_.emplace_back(&ProcessingPool::worker_func, this, i);
        }
    }

    void stop() {
        running_.store(false);
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
    }

    int processed() const { return processed_.load(); }
};

class StorageVisualizer {
private:
    ConcurrentVector<AggregatedResult>& results_;
    std::string output_file_;
    std::thread thread_;
    std::atomic<bool> running_{true};

    ThreadSafeQueue<std::string> output_queue_;

    void storage_func() {
        std::ofstream file(output_file_);
        file << "sensor_id,mean,min,max,sum,stddev,count,prefix_sum\n";

        while (running_.load()) {
            std::string line;
            if (output_queue_.try_pop(line)) {
                file << line << "\n";
                file.flush();
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }

        std::string line;
        while (output_queue_.try_pop(line)) {
            file << line << "\n";
        }
        file.close();
    }

public:
    StorageVisualizer(ConcurrentVector<AggregatedResult>& results,
                      const std::string& filename)
        : results_(results), output_file_(filename) {}

    void start() {
        thread_ = std::thread(&StorageVisualizer::storage_func, this);
    }

    void enqueue_result(const AggregatedResult& r) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(4)
            << r.sensor_id << ","
            << r.mean << ","
            << r.min_val << ","
            << r.max_val << ","
            << r.sum << ","
            << r.stddev << ","
            << r.count << ","
            << r.prefix_sum;
        output_queue_.push(oss.str());
    }

    void stop() {
        running_.store(false);
        output_queue_.stop();
        if (thread_.joinable()) thread_.join();
    }
};

void demonstrate_barrier(int num_threads) {
    std::cout << "\n=== Демонстрация std::barrier (фазы обработки) ===\n";

    std::vector<double> shared_data(100);
    std::iota(shared_data.begin(), shared_data.end(), 1.0);

    std::atomic<double> global_sum{0.0};

    std::barrier sync_point(num_threads, [&]() noexcept {
        std::cout << "[Barrier] Все потоки завершили фазу. Промежуточная сумма: "
                  << global_sum.load() << "\n";
    });

    auto phase_worker = [&](int id) {
        int chunk = shared_data.size() / num_threads;
        int start = id * chunk;
        int end = (id == num_threads - 1) ? shared_data.size() : start + chunk;

        double local_sum = 0;
        for (int i = start; i < end; ++i) {
            local_sum += shared_data[i];
        }
        double expected = global_sum.load();
        while (!global_sum.compare_exchange_weak(expected, expected + local_sum));

        std::cout << "[Фаза 1 - Сбор] Поток " << id
                  << ": локальная сумма = " << local_sum << "\n";

        sync_point.arrive_and_wait();

        double total = global_sum.load();
        for (int i = start; i < end; ++i) {
            shared_data[i] /= total;
        }

        std::cout << "[Фаза 2 - Анализ] Поток " << id
                  << ": нормализация выполнена\n";

        sync_point.arrive_and_wait();

        std::cout << "[Фаза 3 - Вывод] Поток " << id
                  << ": первый элемент = " << shared_data[start] << "\n";
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(phase_worker, i);
    }
    for (auto& t : threads) t.join();

    std::cout << "=== Barrier демонстрация завершена ===\n\n";
}

void demonstrate_execution_policies() {
    std::cout << "=== Демонстрация политик выполнения ===\n";

    const int N = 1000000;
    std::vector<double> data(N);
    std::mt19937 rng(123);
    std::uniform_real_distribution<double> dist(0.0, 100.0);
    for (auto& d : data) d = dist(rng);

    auto t1 = std::chrono::high_resolution_clock::now();
    double sum_seq = std::reduce(std::execution::seq, data.begin(), data.end(), 0.0);
    auto t2 = std::chrono::high_resolution_clock::now();
    auto dur_seq = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

    t1 = std::chrono::high_resolution_clock::now();
    double sum_par = std::reduce(std::execution::par, data.begin(), data.end(), 0.0);
    t2 = std::chrono::high_resolution_clock::now();
    auto dur_par = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

    t1 = std::chrono::high_resolution_clock::now();
    double sum_par_unseq = std::reduce(std::execution::par_unseq, data.begin(), data.end(), 0.0);
    t2 = std::chrono::high_resolution_clock::now();
    auto dur_par_unseq = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  seq       : сумма = " << sum_seq << ", время = " << dur_seq << " мкс\n";
    std::cout << "  par       : сумма = " << sum_par << ", время = " << dur_par << " мкс\n";
    std::cout << "  par_unseq : сумма = " << sum_par_unseq << ", время = " << dur_par_unseq << " мкс\n";
    std::cout << "=== Политики выполнения — завершено ===\n\n";
}

int main() {
    std::cout << "============================================================\n";
    std::cout << " Параллельная система анализа телеметрии\n";
    std::cout << " промышленного оборудования\n";
    std::cout << "============================================================\n\n";

    const int NUM_SENSORS = 120;
    const int SENSOR_THREADS = 4;
    const int PROCESSING_WORKERS = 4;

    std::cout << "Конфигурация:\n";
    std::cout << "  Сенсоров: " << NUM_SENSORS << "\n";
    std::cout << "  Потоков приёма: " << SENSOR_THREADS << "\n";
    std::cout << "  Потоков обработки: " << PROCESSING_WORKERS << "\n";
    std::cout << "  Аппаратных потоков: " << std::thread::hardware_concurrency() << "\n\n";

    ThreadSafeQueue<SensorData> main_queue;

    ConcurrentVector<AggregatedResult> results;

    ProcessingPool processing_pool(PROCESSING_WORKERS, results);
    auto worker_ptrs = processing_pool.get_queue_ptrs();

    Dispatcher dispatcher(main_queue, worker_ptrs);

    SensorInputManager sensor_manager(NUM_SENSORS, SENSOR_THREADS, main_queue);

    StorageVisualizer storage(results, "telemetry_output.csv");

    std::cout << "--- Запуск системы ---\n\n";
    auto start_time = std::chrono::high_resolution_clock::now();

    storage.start();
    processing_pool.start();
    dispatcher.start();
    sensor_manager.start();

    std::this_thread::sleep_for(std::chrono::seconds(2));

    sensor_manager.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    dispatcher.stop();
    processing_pool.stop();

    auto all_results = results.snapshot();
    for (auto& r : all_results) {
        storage.enqueue_result(r);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    storage.stop();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time).count();

    std::cout << "\n--- Результаты ---\n";
    std::cout << "  Сгенерировано показаний: " << sensor_manager.total_generated() << "\n";
    std::cout << "  Диспетчеризировано: " << dispatcher.dispatched() << "\n";
    std::cout << "  Обработано: " << processing_pool.processed() << "\n";
    std::cout << "  Агрегированных результатов: " << all_results.size() << "\n";
    std::cout << "  Общее время: " << total_ms << " мс\n\n";

    std::cout << "--- Примеры агрегированных данных ---\n";
    std::cout << std::fixed << std::setprecision(4);
    int show_count = std::min((int)all_results.size(), 10);
    for (int i = 0; i < show_count; ++i) {
        auto& r = all_results[i];
        std::cout << "  Сенсор " << std::setw(3) << r.sensor_id
                  << " | mean=" << std::setw(8) << r.mean
                  << " | min=" << std::setw(8) << r.min_val
                  << " | max=" << std::setw(8) << r.max_val
                  << " | stddev=" << std::setw(8) << r.stddev
                  << " | count=" << r.count << "\n";
    }

    demonstrate_barrier(4);

    demonstrate_execution_policies();

    std::cout << "============================================================\n";
    std::cout << " Система завершила работу. Результаты в telemetry_output.csv\n";
    std::cout << "============================================================\n";

    return 0;
}
