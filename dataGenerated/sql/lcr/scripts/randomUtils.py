import random, time, math
class RandomUtils:
    @classmethod
    def randomInt(cls,a,b):
        return random.randint(a,b)

    @classmethod
    def randomUrlname(cls,namelen):
        name = ''
        for i in range(namelen):
            if random.randint(0,1) == 0:
                name += chr(random.randint(0,25)+ ord('A'))
            else:
                name += chr(random.randint(0,25)+ ord('a'))
        return name

    #2001 -> now
    @classmethod
    def randomDate(cls):
        sec = 1000000000 + int(random.random()*time.time())
        return sec

    @classmethod
    def randomBase(cls):
        return random.random()

    @classmethod
    def randomFloat(cls,x):
        return random.random() * x

    #from knuth -- return a sample from a (0,1) normal distributon
    @classmethod
    def randomNormal(cls):
        s = 2
        v1 = v2 = 0
        while s >= 1 or s == 0:
            u1 = cls.randomBase()
            u2 = cls.randomBase()
            v1 = 2.0 * u1-1.0
            v2 = 2.0 * u2-1.0
            s = v1 * v1 + v2 * v2
        x1 = v1 * math.sqrt((-2.0 * math.log(s))/s)
        return x1
