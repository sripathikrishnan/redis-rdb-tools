import string
import redis
import random

r = redis.StrictRedis()

def main():
    for x in range(5000):
        create_list_of_words()

def create_list_of_words():
    key = random_string(random.randint(10,25))
    num_items = random.randint(5, 1000)
    pipe = r.pipeline()
    for x in range(num_items):
        value = random_string(random.randint(5, 100))
        pipe.rpush(key, value)
    pipe.execute()
    
def random_string(length) :
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(length))

if __name__ == '__main__':
    main()
    
