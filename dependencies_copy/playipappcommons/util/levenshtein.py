import numpy

accent_cost = 0.1

charMap =\
{
    ("a","á") : accent_cost,
    ("a","â") : accent_cost,
    ("a","ã") : accent_cost,
    ("a","à") : accent_cost,
    ("a","ä") : accent_cost,
    ("e", "é"): accent_cost,
    ("e", "ê"): accent_cost,
    ("e", "ẽ"): accent_cost,
    ("e", "è"): accent_cost,
    ("e", "ë"): accent_cost,
    ("i", "í"): accent_cost,
    ("i", "î"): accent_cost,
    ("i", "ĩ"): accent_cost,
    ("i", "ì"): accent_cost,
    ("i", "ï"): accent_cost,
    ("o", "ó"): accent_cost,
    ("o", "ô"): accent_cost,
    ("o", "õ"): accent_cost,
    ("o", "ò"): accent_cost,
    ("o", "ö"): accent_cost,
    ("u", "ú"): accent_cost,
    ("u", "û"): accent_cost,
    ("u", "ũ"): accent_cost,
    ("u", "ù"): accent_cost,
    ("u", "ü"): accent_cost,
    ("c", "ç"): accent_cost,

}

def levenshteinDistanceDP(token1, token2):
    distances = numpy.zeros((len(token1) + 1, len(token2) + 1))

    for t1 in range(len(token1) + 1):
        distances[t1][0] = t1

    for t2 in range(len(token2) + 1):
        distances[0][t2] = t2

    a = 0
    b = 0
    c = 0

    for t1 in range(1, len(token1) + 1):
        for t2 in range(1, len(token2) + 1):
            if (token1[t1 - 1] == token2[t2 - 1]):
                distances[t1][t2] = distances[t1 - 1][t2 - 1]
            else:

                rep_cost = 1
                if token1[t1 - 1] < token2[t2 - 1]:
                    lc = token1[t1 - 1]
                    hc = token2[t2 - 1]
                else:
                    hc = token1[t1 - 1]
                    lc = token2[t2 - 1]

                if (lc, hc) in charMap:
                    rep_cost = charMap[(lc ,hc)]

                a = distances[t1][t2 - 1] + 1
                b = distances[t1 - 1][t2] + 1
                c = distances[t1 - 1][t2 - 1] + rep_cost


                if (a <= b and a <= c):
                    distances[t1][t2] = a
                elif (b <= a and b <= c):
                    distances[t1][t2] = b
                else:
                    distances[t1][t2] = c

    #printDistances(distances, len(token1), len(token2))
    return distances[len(token1)][len(token2)]

if __name__ == "__main__":
    print(levenshteinDistanceDP("Jurassico", "aJurássico"))
    print(levenshteinDistanceDP("Jorássico", "aJurássico"))
    print(levenshteinDistanceDP("Jurássico", "aJurássico"))
    print(levenshteinDistanceDP("Chácara Vitápolis", "Chacara Vitapolis"))