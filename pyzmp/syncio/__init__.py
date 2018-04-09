"""
This package aims at implementing a TRIVIAL asyncio implementation, without coroutines,
and therefore usable on python2.
We will follow python3 asyncio API, but using normal functions instead of coroutines.
We will assume that :
 - the function will eventually end
We will enforce that :
 - only one function can be passed to an eventloop
 - we can have only one task at a time 

"""