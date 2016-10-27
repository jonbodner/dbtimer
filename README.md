# dbtimer
A small library for timing database interactions

To use:

```go
	dbtimer.SetTimerLoggerFunc(func(ti dbtimer.TimerInfo) {
		fmt.Printf("%s %s %v %v %d\n",ti.Method, ti.Query, ti.Args, ti.Err, ti.End.Sub(ti.Start).Nanoseconds()/1000)
	})
	db, err := sql.Open("timer", "postgres postgres://jon:jon@localhost/jon?sslmode=disable")
```

Then use the db as expected.
