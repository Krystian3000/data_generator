from generator import data_generator
import sys


if __name__ == '__main__':

    args = sys.argv
    if len(args) > 1:
        print(args[1])
        if args[1] == '1':
            gen = data_generator(1)
        elif args[1] == '2':
            gen = data_generator(2)
    print('done')
    sys.exit()
    
