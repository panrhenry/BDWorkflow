

number=1
# 重置，从图01开始重新截图
def r2():
    # 如果在下一行，没有global 声明全局变量，则调用该函数，全局变量不起作用！
    # global number
    number = 2
    print(number)


def r3():
    global number
    number = 3
    print(number)



if __name__ == '__main__':
    r2()
    r3()
    print(number)