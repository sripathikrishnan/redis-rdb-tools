import string
import redis
import random

r = redis.StrictRedis()

def main():
    for x in range(5000):
        create_sorted_set()

def create_sorted_set():
    key = random_string(random.randint(10,25))
    num_items = random.randint(5, 1000)
    pipe = r.pipeline()
    for x in range(num_items):
        value = random_string(random.randint(5, 100))
        score = random_double()
        pipe.zadd(key, score, value)
    pipe.execute()
    
def random_string(length) :
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))

def random_double():
    numerator = float(random.randint(0, 0xffff))
    denominator = float(random.randint(0, 0xffff))
    if denominator > 0 :
        return numerator / denominator
    else :
        return numerator
    
if __name__ == '__main__':
    main()
    
