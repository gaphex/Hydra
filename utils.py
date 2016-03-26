import sys

def decorate(this, n, d):
    s21 = int((n - len(this))/2) * d
    s22 = (n - len(this) - len(s21)) * d
    s1 = n * d
    print ''
    print s1
    print s21 + this + s22
    print s1


def progress(i, n, skip=100, mode=1):
    if mode == 1 and (i%skip == 0 or n < i + skip):
        if i + 1 < n:
            out = "\r%s%%" % "{:5.2f}".format(100*i/float(n))
        elif i == n:
            out = "\r100%\n"
        else:
            out = "\r100%"


        sys.stdout.write(out)
        sys.stdout.flush()

    if i%skip == 0 and mode == 2:
        sys.stdout.write("\r%s" % str(i))
        sys.stdout.flush()
