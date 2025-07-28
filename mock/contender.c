#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
// 这个程序会：
//创建一个全局的、非原子性的锁（用一个 volatile int 变量模拟）。
//创建大量（例如 10 个）“等待者”线程。
//每个“等待者”线程都会进入一个函数调用链，最终在一个无限循环里不断检查锁是否被释放（这就是“忙等待”）。
//一旦某个线程获得锁，它会模拟一小段“工作”（通过 sleep），然后释放锁，让其他线程继续争抢。
// 全局锁，使用 volatile 告诉编译器不要优化掉对这个变量的重复读取
// 0: 未锁定, 1: 已锁定
volatile int g_lock = 0;

// 函数3: 忙等待循环，检查并尝试获取锁
// 这是所有等待线程将花费大量CPU时间的地方
void check_and_acquire_lock() {
    // 忙等待：只要锁被占用，就在这个循环里空转
    while (g_lock == 1) {
        // 在实际的高性能代码中，这里可能还会有 PAUSE 指令等
        // 但对于演示，空循环就足够消耗CPU了
    }
    // 尝试获取锁
    g_lock = 1;
}

// 函数2: 封装了锁的等待逻辑
// 这是为了构建一个更深的、更有意义的函数调用栈
void wait_for_lock() {
    check_and_acquire_lock();
}

// 函数1: 线程的主工作逻辑
void* waiter_thread_main(void* arg) {
    long thread_id = (long)arg;

    printf("线程 %ld 开始尝试获取锁。\n", thread_id);

    // 进入等待锁的函数调用
    wait_for_lock();

    // --- 成功获取锁 ---
    printf("线程 %ld 成功获取锁，开始工作...\n", thread_id);

    // 模拟持有锁并进行工作的时间
    sleep(1); 

    printf("线程 %ld 工作完成，释放锁。\n", thread_id);
    // 释放锁
    g_lock = 0;
    // --- 锁已释放 ---

    return NULL;
}

int main() {
    const int NUM_THREADS = 10;
    pthread_t threads[NUM_THREADS];

    printf("主程序启动，将创建 %d 个线程来争抢一个锁。\n", NUM_THREADS);

    // 创建多个线程
    for (long i = 0; i < NUM_THREADS; i++) {
        if (pthread_create(&threads[i], NULL, waiter_thread_main, (void*)i) != 0) {
            perror("无法创建线程");
            return 1;
        }
    }

    // 等待所有线程执行完毕
    for (int i = 0; i < NUM_THREADS; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            perror("无法加入线程");
            return 1;
        }
    }

    printf("所有线程已执行完毕。主程序退出。\n");

    return 0;
}
