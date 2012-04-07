import redis
import time

def main():
    r = redis.StrictRedis()
    curr_memory = prev_memory = r.info()['used_memory']
    while True:
        if prev_memory != curr_memory:
            print('Delta Memory : %d, Total Memory : %d' % ((curr_memory - prev_memory), curr_memory))
        
        time.sleep(1)
        prev_memory = curr_memory
        curr_memory = r.info()['used_memory']
    
if __name__ == '__main__':
    main()

