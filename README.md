# Hist-min sketch

Hist-min sketch is a probabilistic (only subtly in this implementation) data store for histograms. It can store and return pointwise approximately correct histograms for a single numerical variable. The accuracy is defined by the underlying count-min sketch, the information [here](https://c25l.gitlab.io/2016/04/the-hist-min-sketch/). For a version that handles large volumes of time efficiently as well, consider [this](ttps://c25l.gitlab.io/2017/06/the-hist-min-sketch---time/)

The underlying sketch technology lets you store histograms segmented across several dimensions (think doing performance analytics by technology, geography, etc) while storing only a constant times the nth root of the number of histograms you'd expect to. This implies a dramatic reduction in the storage required.

They're fast and small, they can be prone to errors occasionally, but the more dimensions involved in any query, the fewer errors will be noticed.

## Features
The Hist-min sketch can:
1. Store histograms segmented along many categorical dimensions
2. Incredibly space efficient.
5. Items can be added cheaply
6. Items can be removed cheaply.
7. Queries can be made across any collection of dimensions
8. These queries can be for histograms or quantiles
9. The error implications are controlled.

It is good for streaming data analytics, and for compressing a batch analytics problem into one that can be handled in memory.


## Comparisons
### Uncompressed

So, let's suppose you have a collection of `n` variables (think browser, endpoint, country, etc) `V_1...V_n` and each variable `V_k` has `v_k` values that it takes on. The Hist-min sketch reduces the space complexity from `(v_1+1)*...*(v_n+1)` to `(v_1+...+v_n)`.


What does that mean in practice? Say you wanted to keep a 10-variable histogram, the variables having each, let's say 6 values. You want 1000 bins and ints are 32bits. How much space is required?

`(6+1)^10/120*1000*32 = 8.4 TB` Probably not worth doing.
With the hmst, it's `10*6*10*1000*32 = 18.7MB`. The author considers this a more desirable size.

### Count-min sketches
One might allege that this is a count min sketch. It IS based on a count-min sketch conceptually, yes, but it's different in a lot of ways. 

1. The count min sketch for histograms would involve hashing all the values separately, rather than together, so there's a potential for a low-value high-value hash collision which doesn't exist here.
2. Count min sketches use a fixed number of hash functions, and here it depends on the query in question.
3. Count min sketches do not support cancellation natively, but the HMS and HMST do, things can be safely removed from the sketch. The error implications are well understood.
4. The CMS error rate will climb as items are added to it, the HMS error rate, in this implementation, will only climb as disjointed metrics are added.