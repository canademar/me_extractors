import math 

def ig(vector):
    n = len(vector)
    probs = {}
    for e in vector:
        if e not in probs.keys():
            probs[e] = float(vector.count(e))
    acum = 0
    print probs
    for k in probs:
        acum += math.pow((probs[k]/n) , 2.0)
    return 1 - acum


print ig([1,2,3,4,5,6,7,8])
print ig([1,1,2,1,1,2,3,1])
print ig([1,1,1,1,1,1,1,1])



