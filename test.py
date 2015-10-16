import sys

while(1):
    print("a")
    sys.stdout.writeline("about to read a line")
    print("b")
    sys.stdout.flush()
    #line=sys.stdin.readline()
    print("c")
    sys.stdout.writeline("thanks!")
    print("d")
    sys.stdout.flush()
    print("e")
