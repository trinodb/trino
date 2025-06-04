# Mathematical functions and operators

(mathematical-operators)=
## Mathematical operators

| Operator | Description                                     |
| -------- | ----------------------------------------------- |
| `+`      | Addition                                        |
| `-`      | Subtraction                                     |
| `*`      | Multiplication                                  |
| `/`      | Division (integer division performs truncation) |
| `%`      | Modulus (remainder)                             |

## Mathematical functions

:::{function} abs(x) -> [same as input]
Returns the absolute value of `x`.
:::

:::{function} cbrt(x) -> double
Returns the cube root of `x`.
:::

:::{function} ceil(x) -> [same as input]
This is an alias for {func}`ceiling`.
:::

:::{function} ceiling(x) -> [same as input]
Returns `x` rounded up to the nearest integer.
:::

:::{function} degrees(x) -> double
Converts angle `x` in radians to degrees.
:::

:::{function} e() -> double
Returns the constant Euler's number.
:::

:::{function} exp(x) -> double
Returns Euler's number raised to the power of `x`.
:::

:::{function} floor(x) -> [same as input]
Returns `x` rounded down to the nearest integer.
:::

:::{function} ln(x) -> double
Returns the natural logarithm of `x`.
:::

:::{function} log(b, x) -> double
Returns the base `b` logarithm of `x`.
:::

:::{function} log2(x) -> double
Returns the base 2 logarithm of `x`.
:::

:::{function} log10(x) -> double
Returns the base 10 logarithm of `x`.
:::

:::{function} mod(n, m) -> [same as input]
Returns the modulus (remainder) of `n` divided by `m`.
:::

:::{function} pi() -> double
Returns the constant Pi.
:::

:::{function} pow(x, p) -> double
This is an alias for {func}`power`.
:::

:::{function} power(x, p) -> double
Returns `x` raised to the power of `p`.
:::

:::{function} radians(x) -> double
Converts angle `x` in degrees to radians.
:::

:::{function} round(x) -> [same as input]
Returns `x` rounded to the nearest integer.
:::

:::{function} round(x, d) -> [same as input]
:noindex: true

Returns `x` rounded to `d` decimal places.
:::

:::{function} sign(x) -> [same as input]
Returns the signum function of `x`, that is:

- 0 if the argument is 0,
- 1 if the argument is greater than 0,
- -1 if the argument is less than 0.

For floating point arguments, the function additionally returns:

- -0 if the argument is -0,
- NaN if the argument is NaN,
- 1 if the argument is +Infinity,
- -1 if the argument is -Infinity.
:::

:::{function} sqrt(x) -> double
Returns the square root of `x`.
:::

:::{function} truncate(x) -> double
Returns `x` rounded to integer by dropping digits after decimal point.
:::

:::{function} width_bucket(x, bound1, bound2, n) -> bigint
Returns the bin number of `x` in an equi-width histogram with the
specified `bound1` and `bound2` bounds and `n` number of buckets.
:::

:::{function} width_bucket(x, bins) -> bigint
:noindex: true

Returns the bin number of `x` according to the bins specified by the
array `bins`. The `bins` parameter must be an array of doubles and is
assumed to be in sorted ascending order.
:::

## Random functions

:::{function} rand() -> double
This is an alias for {func}`random()`.
:::

:::{function} random() -> double
Returns a pseudo-random value in the range 0.0 \<= x \< 1.0.
:::

:::{function} random(n) -> [same as input]
:noindex: true

Returns a pseudo-random number between 0 and n (exclusive).
:::

:::{function} random(m, n) -> [same as input]
:noindex: true

Returns a pseudo-random number between m and n (exclusive).
:::

## Trigonometric functions

All trigonometric function arguments are expressed in radians.
See unit conversion functions {func}`degrees` and {func}`radians`.

:::{function} acos(x) -> double
Returns the arc cosine of `x`.
:::

:::{function} asin(x) -> double
Returns the arc sine of `x`.
:::

:::{function} atan(x) -> double
Returns the arc tangent of `x`.
:::

:::{function} atan2(y, x) -> double
Returns the arc tangent of `y / x`.
:::

:::{function} cos(x) -> double
Returns the cosine of `x`.
:::

:::{function} cosh(x) -> double
Returns the hyperbolic cosine of `x`.
:::

:::{function} sin(x) -> double
Returns the sine of `x`.
:::

:::{function} sinh(x) -> double
Returns the hyperbolic sine of `x`.
:::

:::{function} tan(x) -> double
Returns the tangent of `x`.
:::

:::{function} tanh(x) -> double
Returns the hyperbolic tangent of `x`.
:::

## Geometric functions

:::{function} cosine_distance(array(double), array(double)) -> double
Calculates the cosine distance between two dense vectors:

```sql
SELECT cosine_distance(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0]);
-- 0.01613008990009257
```
:::

:::{function} cosine_similarity(array(double), array(double)) -> double
Calculates the cosine similarity of two dense vectors:

```sql
SELECT cosine_similarity(ARRAY[1.0, 2.0], ARRAY[3.0, 4.0]);
-- 0.9838699100999074
```
:::

:::{function} cosine_similarity(x, y) -> double
:no-index:
Calculates the cosine similarity of two sparse vectors:

```sql
SELECT cosine_similarity(MAP(ARRAY['a'], ARRAY[1.0]), MAP(ARRAY['a'], ARRAY[2.0]));
-- 1.0
```
:::

## Floating point functions

:::{function} infinity() -> double
Returns the constant representing positive infinity.
:::

:::{function} is_finite(x) -> boolean
Determine if `x` is finite.
:::

:::{function} is_infinite(x) -> boolean
Determine if `x` is infinite.
:::

:::{function} is_nan(x) -> boolean
Determine if `x` is not-a-number.
:::

:::{function} nan() -> double
Returns the constant representing not-a-number.
:::

## Base conversion functions

:::{function} from_base(string, radix) -> bigint
Returns the value of `string` interpreted as a base-`radix` number.
:::

:::{function} to_base(x, radix) -> varchar
Returns the base-`radix` representation of `x`.
:::

## Statistical functions

:::{function} t_pdf(x, df) -> double
Computes the Student's t-distribution probability density function for given x and
degrees of freedom (df). The x must be a real value and degrees of freedom must be
an integer and positive value.
:::

:::{function} wilson_interval_lower(successes, trials, z) -> double
Returns the lower bound of the Wilson score interval of a Bernoulli trial process
at a confidence specified by the z-score `z`.
:::

:::{function} wilson_interval_upper(successes, trials, z) -> double
Returns the upper bound of the Wilson score interval of a Bernoulli trial process
at a confidence specified by the z-score `z`.
:::

## Cumulative distribution functions

:::{function} beta_cdf(a, b, v) -> double
Compute the Beta cdf with given a, b parameters:  P(N \< v; a, b).
The a, b parameters must be positive real numbers and value v must be a real value.
The value v must lie on the interval \[0, 1\].
:::

:::{function} inverse_beta_cdf(a, b, p) -> double
Compute the inverse of the Beta cdf with given a, b parameters for the cumulative
probability (p): P(N \< n). The a, b parameters must be positive real values.
The probability p must lie on the interval \[0, 1\].
:::

:::{function} inverse_normal_cdf(mean, sd, p) -> double
Compute the inverse of the Normal cdf with given mean and standard
deviation (sd) for the cumulative probability (p): P(N \< n). The mean must be
a real value and the standard deviation must be a real and positive value.
The probability p must lie on the interval (0, 1).
:::

:::{function} normal_cdf(mean, sd, v) -> double
Compute the Normal cdf with given mean and standard deviation (sd):  P(N \< v; mean, sd).
The mean and value v must be real values and the standard deviation must be a real
and positive value.
:::

:::{function} t_cdf(x, df) -> double
Compute the Student's t-distribution cumulative density function for given x and degrees of freedom (df).
The x must be a real value and degrees of freedom must be an integer and positive value.
:::
