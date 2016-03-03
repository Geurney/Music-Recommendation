import sys
import math

from pyspark import SparkContext

def ua_flat_doc(document):
    return document[1].rstrip( ).split(' ')

def ua_map(line):
    line_split = line.split('\t')
    key = line_split[0]
    value = [(line_split[1],line_split[2])]
    return (key, value)  

def ua_reduce(value_a, value_b):
    value = list(value_a)
    value.extend(value_b)
    return value

def ua_flat_vec(tuple):
    value = tuple[1]
    tuples=[]
    n = len(value)
    for i in range(0, n):
        A = value[i]
        for j in range(0, n):
            B = value[j]
            tuples.append(((A[0], B[0]),[int(A[1]),int(B[1]), 1]))
    return tuples

def ua_reduce_vec(value_a, value_b):
    A = value_a[0] + value_b[0]
    B = value_a[1] + value_b[1]
    C = value_a[2] + value_b[2]
    return [A, B, C]

def ua_map_avg(tuple):
    key = tuple[0]
    avg_a = tuple[1][0] / tuple[1][2]
    avg_b = tuple[1][1] / tuple[1][2]
    return (key, [avg_a, avg_b])

def ua_map_cmp(tuple):
    key = tuple[0]
    global avg_matrix
    avg = avg_matrix[key]
    m_a = tuple[1][0] - avg[0]
    m_b = tuple[1][1] - avg[1]
    c_a = m_a * m_a
    c_b = m_b * m_b
    return (key, [m_a* m_b, c_a, c_b])

def ua_reduce_cmp(value_a, value_b):
    A = value_a[0] + value_b[0]
    B = value_a[1] + value_b[1]
    C = value_a[2] + value_b[2]
    return [A, B, C]

def ua_map_cmp_final(tuple):
    print 'HERE!'
    print tuple
    print tuple[1][1] * tuple[1][2]
    key = tuple[0]
    value = tuple[1][0] / ((math.sqrt(tuple[1][1] * tuple[1][2])+ 1))
    return (key, value)

def user_artist_matrix(file_name, output="user_artist_matrix.out"):
    sc = SparkContext("local[8]", "UserArtistMatrix")
    """ Reads in a sequence file FILE_NAME to be manipulated """
    file = sc.sequenceFile(file_name)

    """
    - flatMap takes in a function that will take one input and outputs 0 or more
      items
    - map takes in a function that will take one input and outputs a single item
    - reduceByKey takes in a function, groups the dataset by keys and aggregates
      the values of each key
    """
    ua_matrix = file.flatMap(ua_flat_doc) \
                 .map(ua_map) \
                 .reduceByKey(ua_reduce) \
                 .sortByKey(keyfunc=lambda k: int(k))

    ua_matrix = ua_matrix.flatMap(ua_flat_vec)

    global avg_matrix
    avg_matrix = ua_matrix.reduceByKey(ua_reduce_vec) \
                         .map(ua_map_avg)
    
    avg_matrix = avg_matrix.collectAsMap()

    co_matrix = ua_matrix.map(ua_map_cmp) \
                         .reduceByKey(ua_reduce_cmp) \
                         .map(ua_map_cmp_final)

    """ Takes the dataset stored in counts and writes everything out to OUTPUT """
    co_matrix.coalesce(1).saveAsTextFile(output)

""" Do not worry about this """
if __name__ == "__main__":
    argv = sys.argv
    if len(argv) == 2:
        user_artist_matrix(argv[1])
    else:
        user_artist_matrix(argv[1], argv[2])
