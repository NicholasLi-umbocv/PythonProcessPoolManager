import time
import processpool
import processwork


def main():
    # Create our consumer manager, instruct it to manage 10 workers with the target action of create_worker
    manager = processpool.PoolManager(10, processwork.create_worker)
    # Boot up
    manager.run()
    # Check monitor process status
    print(manager.monitor_is_alive())
    # Get an idle process
    p = manager.get()
    print(p)
    # Send msg to idle process
    manager.tell_process_to_work(p, "I am {}".format(p))
    # Send msg to same process, but this time the process is not idle, so it should get error
    manager.tell_process_to_work(p, "I am {}, 2-nd".format(p))
    # Get another idle process
    p = manager.get()
    print(p)
    # Send die msg to the process
    manager.tell_process_to_work(p, "die")
    time.sleep(20)
    manager.close_processes()


if __name__ == '__main__':
    main()
