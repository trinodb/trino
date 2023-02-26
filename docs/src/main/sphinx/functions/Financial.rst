==============================
Financial Functions and Models
==============================

Functions and models providing financial calculations and analysis. Intended for
educational and illustrative purposes only and should not be used for making
investment decisions. Models may not accurately reflect actual market prices or the
behavior of stock prices in the real world.

Black-Scholes Model
-------------------

.. function:: bsEuropeanOption(stockPrice, strikePrice, timeToExpiration, riskFreeRate, volatility, optionType) -> double

    Returns a numeric expression representing the theoretical price of the option,
    based on the Black-Scholes model.

        SELECT bsEuropeanOption(100, 110, 1, 0.05, 0.2, 'call');
        -- 5.94327393259265
        SELECT bsEuropeanOption(100, 90, 0.5, 0.02, 0.3, 'put');
        -- 5.48771157975784

    ``stockPrice``: A numeric expression representing the current stock price.
    ``strikePrice``: A numeric expression representing the strike price of the option.
    ``timeToExpiration``: A numeric expression representing the time in until expiration of the option, in years.
    ``riskFreeRate``: A numeric expression representing the risk-free interest rate, expressed as a decimal.
    ``volatility``: A numeric expression representing the annualized volatility of the stock, expressed as a decimal.
    ``optionType``: A string expression representing the type of option, either "call" or "put".
