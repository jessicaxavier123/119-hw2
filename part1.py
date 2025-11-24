"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    def mapper(kv):
        k, v = kv
        return f(k, v)
    return rdd.flatMap(mapper)

# Remove skip when implemented!
@pytest.mark.skip
def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(f)

# Remove skip when implemented!
@pytest.mark.skip
def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===
One scenario where having the keys for Map and keys for Reduce be different is useful
is if you have a dataset of city and temperature. During the map stage you might emit
month and temperature so that the reduce stage can compute the average
temperature for each month. Map keys would be cities and reduce keys would be months which
would aggregate differently than the input.

=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input():
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    # This will be referred to in the following questions.
    return sc.parallelize(range(1, 1_000_001))


def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    # You may use general_map or general_reduce here if you like (but you don't have to) to get the total count.
    return rdd.count()

"""
Now use the general_map and general_reduce functions to answer the following questions.

For Q5-Q7, your answers should use general_map and general_reduce as much as possible (wherever possible): you will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value
    def to_kv(number):
        # key=1 (arbitrary, since we only need one total), value=(number, 1)
        return (1, (number, 1))
    
    rdd_kv = rdd.map(to_kv)
    
    # Step 2: Reduce by key to sum both values and counts
    def sum_pairs(a, b):
        # a and b are tuples of (sum, count)
        return (a[0] + b[0], a[1] + b[1])
    
    reduced = general_reduce(rdd_kv, sum_pairs)
    
    # Step 3: Compute average
    total_sum, total_count = reduced.collect()[0][1]
    return total_sum / total_count

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    # Step 1: Convert each number to a (key, value) pair for general_map
    def to_kv(number):
        # key is arbitrary (0), value is the number
        return (0, number)
    
    rdd_kv = rdd.map(to_kv)
    
    # Step 2: Map each number to a list of (digit, 1) pairs
    def number_to_digit_pairs(key, value):
        pairs = []
        for digit in str(value):
            pairs.append((digit, 1))
        return pairs
    
    digits_rdd = general_map(rdd_kv, number_to_digit_pairs)
    
    # Step 3: Reduce by digit to sum counts
    def sum_counts(a, b):
        return a + b
    
    counts_rdd = general_reduce(digits_rdd, sum_counts)
    
    # Step 4: Collect counts and find most and least frequent digits
    counts = counts_rdd.collect()  # list of (digit, frequency)
    
    # Helper functions to find max and min by frequency
    def max_by_freq(pairs):
        max_pair = pairs[0]
        for pair in pairs[1:]:
            if pair[1] > max_pair[1]:
                max_pair = pair
        return max_pair
    
    def min_by_freq(pairs):
        min_pair = pairs[0]
        for pair in pairs[1:]:
            if pair[1] < min_pair[1]:
                min_pair = pair
        return min_pair
    
    most_digit, most_freq = max_by_freq(counts)
    least_digit, least_freq = min_by_freq(counts)
    
    return (most_digit, most_freq, least_digit, least_freq)

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.

Please implement this without using an external library!
You should write this from scratch in Python.

Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use all lowercase letters.
- The word "and" should only appear after the "hundred" part, and nowhere else.
  It should appear after the hundreds if there are tens or ones in the same block.
  (Note the 1001 case above which differs from some other implementations!)
"""

# *** Define helper function(s) here ***
def number_to_words(n):
    if n == 0:
        return "zero"
    if n == 1_000_000:
        return "one million"

    ones = ["", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"]
    teens = ["ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
            "sixteen", "seventeen", "eighteen", "nineteen"]
    tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy",
            "eighty", "ninety"]

    def three_digit_to_words(num):
        if num == 0:
            return []
        part = []
        h = num // 100
        t = num % 100
        if h > 0:
            part.append(ones[h])
            part.append("hundred")
            if t > 0:
                part.append("and")
        if t >= 20:
            part.append(tens[t // 10])
            if t % 10 > 0:
                part.append(ones[t % 10])
        elif t >= 10:
            part.append(teens[t - 10])
        elif t > 0:
            part.append(ones[t])
        return part

    words = []

    if n >= 1000:
        thousands = n // 1000
        remainder = n % 1000
        # recursively convert thousands if >= 1000
        if thousands >= 1000:
            words += number_to_words(thousands).split()
        else:
            words += three_digit_to_words(thousands)
        words.append("thousand")
        if remainder > 0:
            words += three_digit_to_words(remainder)
    else:
        words += three_digit_to_words(n)

    return " ".join(words)

def q7(rdd):
    # Input: the RDD from Q4
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    # Step 1: Convert numbers to words
     # Convert numbers to key-value pairs for general_map
    def to_kv(number):
        return (0, number)

    rdd_kv = rdd.map(to_kv)

    # Map each number to (letter, 1) pairs
    def number_to_letter_pairs(key, value):
        word = number_to_words(value).replace(" ", "").replace("-", "").lower()
        pairs = []
        for letter in word:
            pairs.append((letter, 1))
        return pairs

    letters_rdd = general_map(rdd_kv, number_to_letter_pairs)

    # Reduce by letter to count frequency
    def sum_counts(a, b):
        return a + b

    counts_rdd = general_reduce(letters_rdd, sum_counts)

    # Collect counts
    counts = counts_rdd.collect()

    # Find max and min by frequency
    def max_by_freq(pairs):
        max_pair = pairs[0]
        for pair in pairs[1:]:
            if pair[1] > max_pair[1]:
                max_pair = pair
        return max_pair

    def min_by_freq(pairs):
        min_pair = pairs[0]
        for pair in pairs[1:]:
            if pair[1] < min_pair[1]:
                min_pair = pair
        return min_pair

    most_letter, most_freq = max_by_freq(counts)
    least_letter, least_freq = min_by_freq(counts)

    return (most_letter, most_freq, least_letter, least_freq)

"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.

Notes:
- The functions q8_a and q8_b don't have input parameters; they should call
  load_input_bigger directly.
- Please ensure that each of q8a and q8b runs in at most 3 minutes.
- If you are unable to run up to 100 million on your machine within the time
  limit, please change the input to 10 million instead of 100 million.
  If it is still taking too long even for that,
  you may need to change the number of partitions.
  For example, one student found that setting number of partitions to 100
  helped speed it up.
"""

def load_input_bigger():
    return sc.parallelize(range(1, 100_000_001), 100)

def q8_a():
    # version of Q6
    # It should call into q6() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    rdd = load_input_bigger()
    return q6(rdd)

def q8_b():
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    rdd = load_input_bigger()
    return q7(rdd)

"""
Discussion questions

9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===
k1 was an input map key in the type int
v1 was an input map value in the type int
k2 was a map output or a reduce key in the type string ('0-9' for Q6 and 'a-z' for Q7)
v2 was an map output or a reduce value in the type int
=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===
No, the simplified MapReduce from class would not be sufficient to compute the above. 
Usually, MapReduce maps to many keys and reduces each key independently, which the simplified version cannot do.
In this case, we need to track multiple keys at once (all digits or letters) and compute the counts per key.
=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

For Q11, Q14, and Q16:
your answer should return a Python set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    # Step 1: Map each number to an empty list of key-value pairs
    def empty_map(key, value):
        # Always returns an empty list → no output
        return []
    
    rdd_kv = rdd.map(lambda x: (0, x))  # convert numbers to (k1, v1)
    
    # Step 2: Apply general_map with empty output
    mapped = general_map(rdd_kv, empty_map)
    
    # Step 3: Reduce (does nothing because mapped is empty)
    def dummy_reduce(a, b):
        return a + b  # won't actually be called
    
    reduced = general_reduce(mapped, dummy_reduce)
    
    # Step 4: Return as a set
    return set(reduced.collect())

"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===
In Q11 it outputs an empty list because every mapper returns an empty list for every input
in which the reducer had nothing to process. General reduce depends on the keys that appear 
in mapped data, but nothing special about the data that you use.

=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===
Because general_reduce applies the reduce function f in whatever order Spark happens to process the data. 
The final output can change if f isn't associative or commutative. Since Spark changes and groups values 
differently, the order of reductions isn't fixed, so the result can vary from run to run.
=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    # Map all values to the same key, e.g., key = 1
    # Mapper function: map every value x → (1, x)
    def map_to_single_key(x):
        return (1, x)

    # Non-associative reduce function
    # Subtraction will give different results depending on order
    def subtract(x, y):
        return x - y

    mapped = rdd.map(map_to_single_key)

    reduced = general_reduce(mapped, subtract)

    return set(reduced.collect())
"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===
When I run the pipeline, it produces one key-value pair like (1, 484368374988), 
but the exact value changes on different runs. 
This shows nondeterministic behavior because the reduce function depends on the order in which Spark 
processes the values, and that order can vary due to partitioning and scheduling.
=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1_000_001), numSlices=2)

    def map_to_single_key(x):
        return (1, x)

    def subtract(x, y):
        return x - y

    mapped = rdd.map(map_to_single_key)
    reduced = general_reduce(mapped, subtract)

    return set(reduced.collect())


def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # Medium parallelism (e.g., 8 partitions)
    rdd = sc.parallelize(range(1, 1_000_001), numSlices=8)

    def map_to_single_key(x):
        return (1, x)

    def subtract(x, y):
        return x - y

    mapped = rdd.map(map_to_single_key)
    reduced = general_reduce(mapped, subtract)

    return set(reduced.collect())


def q16_c():
    # For this one, create the RDD yourself. Choose the number of partitions.
    rdd = sc.parallelize(range(1, 1_000_001), numSlices=32)

    def map_to_single_key(x):
        return (1, x)

    def subtract(x, y):
        return x - y

    mapped = rdd.map(map_to_single_key)
    reduced = general_reduce(mapped, subtract)

    return set(reduced.collect())

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===
Yes, the answer was different for the different levels of parallelism:
q16a answer: {(1, 249999000000)}
q16b answer: {(1, 484368374988)}
q16c answer: {(1, 498992906190)}  

=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occurred on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===
Yes, not having the same answer over the different methods of parallelism could
lead to serious problems like wrong analysis and incorrect diagnostics. 
=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===
"A commutative reducer is a pure function and for any sequence of input rows, and any of its permutations holds."
=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    # Example: last-value per group (similar to the Scope script you shared)
    
    # Sample RDD to simulate a grouped dataset
    rdd = sc.parallelize([
        ("A", 1),
        ("A", 2),
        ("A", 3),
        ("B", 10),
        ("B", 20)
    ])

    # Mapper: keep the same key-value pair
    def identity_map(k, v):
        return [(k, v)]

    # Reducer: keep only the last value seen in the group
    def last_value_reduce(a, b):
        return b  # always take the last value

    try:
        mapped = general_map(rdd, identity_map)
        reduced = general_reduce(mapped, last_value_reduce)
        result = reduced.collect()
        # If we reach here, it was possible
        return True
    except:
        # If an error occurs, it was not possible
        return False

"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
