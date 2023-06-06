## bencode

4 types of things

```
numbers            i-[0-9]e
blobs              [0-9]:{data} (number is length) <-- can be strings provided they are utf-8 encoded
dictionaries       d {k, v k, v k, v} e <-- keys must be sorted
lists              l {v, v v v} e
```

typed bencode

```
d
	0:                  <-- first key
	3:pr1               <-- first value
	{ fields for pr1 }
e

or ...

l
	3:pr1
	d
		... (the fields for pr1)
	e
e
```
