def func1():
    try:
        a = 1/0
        return True
    except Exception as e:
        print ('error',e)
        raise 

    finally:
        print('finally')
        return False

def main():
    try:
        func1()
        func1()
        func1()
    except Exception as e:
        print('new error',e)
    finally:
        print('done')

if __name__=="__main__":
    main()