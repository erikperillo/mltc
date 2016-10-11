#!/usr/bin/env python3

import glob

max_pos = 1000
pos_dirs = [
    "/home/erik/20_newsgroup/sci.space/"
]

max_neg = 1618
neg_dirs = [
    "/home/erik/20_newsgroup/sci.med",
    "/home/erik/20_newsgroup/talk.politics.misc"
]

def pre_proc(word):
    word = word.strip()
    word = word.replace("\n", "")
    word = word.replace('"', "")
    word = word.replace("'", "")
    word = word.replace(",", "")
    word = word.replace("-", "")
    word = word.lower()
    return word

def join_lines(lines, sep=" "):
    return sep.join(lines)

def file_to_line(filepath, clase="1.0"):
    with open(filepath) as f:
        lines = [pre_proc(line) for line in f]
        line = join_lines(lines)
        return "%s,%s" % (clase, line)

def main():
    for pos_dir in pos_dirs:
        #print("%s:" % pos_dir)
        for f in glob.glob("%s/*" % pos_dir)[:max_pos//len(pos_dirs)]:
            #print("\t%s" % f)
            try:
                print(file_to_line(f, "1.0"))
            except:
                continue

    for neg_dir in neg_dirs:
        #print("%s:" % neg_dir)
        for f in glob.glob("%s/*" % neg_dir)[:max_neg//len(neg_dirs)]:
            #print("\t%s" % f)
            try:
                print(file_to_line(f, "0.0"))
            except:
                continue

if __name__ == "__main__":
    main()
