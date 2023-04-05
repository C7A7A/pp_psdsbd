/*
  1. Pokazuj informację ilu osobom udało się wejść na szczyt lub zdobyć bazę w ciągu ostatnich 10 sekund dodatkowo grupując dane po rezultacie wyprawy.
      """
      @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
      @name('result')
          select amount_people, result, sum(amount_people) as sum_people, ts
          from MountainEvent(result IN ('summit-reached', 'base-reached'))#time(10)
          group by result;
          """

  2. Wykrywaj przypadki rezygnacji ze wspinaczki z powodu zaginięcia (resignation someone missing) dla wypraw, w których bierze udział mniej niż 3 osoby
      @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
      @name('result')
          select peak_name, trip_leader, result, amount_people
          from MountainEvent(result = "resignation-someone-missing")#length(10)
          having amount_people < 3;"""

  3. Wykrywaj przypadki rezygnacji ze wspinaczki dla grup, w których liczba osób jest największa w ostatnich 20 wyprawach (batch)
      """
      @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
      @name('result')
          select peak_name, trip_leader, result, amount_people, max(amount_people) as max_people, ts
          from MountainEvent(result IN ("resignation-someone-missing", "resignation-injury", "resignation-weather", "resignation-other"))
              #length_batch(20)
          having max(amount_people) = amount_people;"""

  4. Dla każdego alpinisty (istnieje 10 alpinistów w zbiorze danych) znajdź pierwszą wyprawę, która zakończyła się kontuzją.
  Następnie wykrywaj każdą kolejną wyprawę, która zakończyła się kontuzją oraz w wyprawie wzięło udział więcej osób niż w pierwszej nieudanej wyprawie.
      """
      @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
      @name('createWindow')
          create window MountainEventFirstFailed#length(10) as MountainEvent;

      @name('fillWindow')
          insert into MountainEventFirstFailed
          select *
          from MountainEvent(result = "resignation-injury")#firstunique(trip_leader);

      @name('result')
          select meff.trip_leader, meff.result, meff.amount_people, me.trip_leader, me.amount_people
          from MountainEvent(result = "resignation-injury")#length(1) as me, MountainEventFirstFailed as meff
          where me.amount_people > meff.amount_people and me.trip_leader = meff.trip_leader;"""

  5. Wykrywaj co najmniej 4 kolejne wyprawy, które zakończyły się sukcesem w ciągu 1 sekundy.

  select a[0].ts as ts1, a[1].ts as ts2, a[2].ts as ts3, a[3].ts as ts4,
  a[0].trip_leader as tl1, a[1].trip_leader as tl2, a[2].trip_leader as tl3, a[3].trip_leader as tl4,
  a[0].result as r1, a[1].result as r2, a[2].result as r3, a[3].result as r4,
  b.result as result_end
  from pattern [
      every ([4:] a=MountainEvent(result in ("summit-reached", "base-reached"))
          until b=MountainEvent(result in ("resignation-injury", "resignation-weather", "resignation-someone-missing", "resignation-other"))
          where timer:within(1 sec)
      )
  ]

  6. Wykrywaj przypadki liderów wypraw, których wyprawy zakończyły się trzykrotnie niepowodzeniem lub dwukrotnie niepowodzeniem, a ostatnia wypraw udała się.
  W przypadku trzykrotnego niepowodzenia, wykrywaj takie wyprawy, że w kazdej kolejnej brało udział WIĘCEJ osób.
  W przypadku dwukrotnego niepowodzenia, a nastepnie powodzenia wykrywaj wyprawy takie, że:
  a) w DRUGIEJ wyprawie brało udział WIĘCEJ osób niz w PIERWSZEJ
  b) w TRZECIEJ wyprawie brało udział MNIEJ osób niż w PIERWSZEJ

  select a.trip_leader as leader,
  a.result as AResult, b.result as BResult, c.result as CResult, d.result as DResult,
  a.amount_people, b.amount_people, c.amount_people, d.amount_people
  from pattern [
      every (
          a=MountainEvent(result not in ("summit-reached", "base-reached")) ->
          b=MountainEvent(b.trip_leader = a.trip_leader and result not in ("summit-reached", "base-reached") and b.amount_people > a.amount_people) -> (
              c=MountainEvent(c.trip_leader = a.trip_leader and result not in ("summit-reached", "base-reached") and c.amount_people > b.amount_people) or
              d=MountainEvent(d.trip_leader = a.trip_leader and result in ("summit-reached", "base-reached") and d.amount_people < a.amount_people)
          )
      )
  ]

  7. Dla każdego lidera wykrywaj takie wyprawy, że:
  a) pierwsze wyprawa się nie udała
  b) od 3-5 kolejnych wypraw udaje się oraz bierze w nich udział mniej osób niż w pierwszej wyprawie
  c) ostatnie wyprawa nie udaje się oraz bierze w niej udział tyle samo lub więcej osób niż w pierwszej wyprawie

  select * from MountainEvent
  match_recognize (
      partition by trip_leader
      measures
          A.amount_people as Appl, A.trip_leader as tripLeader,
          first(B.amount_people) as firstBppl,
          C.amount_people as Cppl,
          A.result as Ares, first(B.result) as Bres, C.result as Cres
      pattern (A B{3, 5} C)
      define
          A as A.result not in ("summit-reached", "base-reached"),
          B as B.amount_people < A.amount_people and B.result in ("summit-reached", "base-reached"),
          C as C.amount_people >= A.amount_people and C.result not in ("summit-reached", "base-reached")
  )
*/