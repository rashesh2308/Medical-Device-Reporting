import threading


def a():
    i = 1
    while (i):
        z = i + 1
        print(z)


def b():
    print('b')

print('aaaaaaaaaaaaaaaaaaaaaaaa')
t1 = threading.Thread(target=a)
print('11111')
t2 = threading.Thread(target=b)
print('aaaaaa')
t1.start()
print('sssss')
t2.start()