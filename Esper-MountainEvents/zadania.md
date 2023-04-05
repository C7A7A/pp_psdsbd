# Charakterystyka danych
Mountain, Mountaineering

Dane dotyczą zdarzeń, które mają miejsce podczas wypraw górskich. Każdy wiersz składa się z nazwy szczytu, lidera wyprawy, rezultatu wyprawy, liczby osób, która bierze udział w wyprawie oraz daty zakończenia wyprawy (timestamp).

W strumieniu pojawiają się zdarzenia zgodne ze schematem `MountainEvent`.

```
create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string, its string);
```


Dane są uzupełnione są o dwie etykiety czasowe. 
* Pierwsza (`ts`) związana jest z momentem zakończenia wyprawy. Etykieta ta może się losowo spóźniać w stosunku do czasu systemowego max do 42 sekund.
* Druga (`its`) związana jest z momentem rejestracji zdarzenia zakończenia wyprawy w systemie.

# Opis atrybutów

Atrybuty w każdym zdarzeniu zgodnym ze schematem `MountainEvent` mają następujące znaczenie:

* `peak_name`
    - typ: string
    - znaczenie: nazwa szczytu
    - kategoria: atrybut opisujący zdarzenie
* `trip_leader`
    - typ: string
    - znaczenie: imię i nazwisko lidera wyprawy (znany alpinista/alpinistka)
    - kategoria: atrybut po którym można dane grupować
* `result`
    - typ: string
    - znaczenie: rezultat wyprawy
    - kategoria: atrybut po którym można dane grupować (summit reached, base reached, resignation injury, resignation weather, resignation someone    missing, resignation other)
* `amount_people`
    - typ: int
    - znaczenie: liczba osób biorąca udział w wyprawie
    - kategoria: atrybut którego wartości można agregować
* `ts`
    - typ: string
    - znaczenie: moment zakończenia wyprawy
    - kategoria: znacznik czasowy zdarzeń
* `its`
    - typ: string
    - znaczenie: czas rejestracji faktu zakończenia wyprawy
    - kategoria: znacznik czasowy zdarzeń


# Zadania
Opracuj rozwiązania poniższych zadań. 
* Opieraj się na strumieniu zdarzeń zgodnych ze schematem `MountainEvent`
* W każdym rozwiązaniu możesz skorzystać z jednego lub kilku poleceń EPL.
* Ostatnie polecenie będące ostatecznym rozwiązaniem zadania musi 
  * być poleceniem `select` 
  * posiadającym etykietę `answer`, przykładowo:
  ```
    @name('answer') 
    select trip_leader, result, amount_people, ts from MountainEvent#length(1);
  ```

## Zadanie 1
Pokazuj informację ilu osobom udało się wejść na szczyt lub zdobyć bazę w ciągu ostatnich 10 sekund dodatkowo grupując dane po rezultacie wyprawy.

Wyniki powinny zawierać, następujące kolumny:
- `amount_people` - liczba osób biorąca udział w wyprawie
- `result` - rezultat wyprawy
- `sum_people` - suma osób, którym udało się wejść na szczyt w ciągu ostatnich 10 sekund
- `ts` - czas rejestracji faktu zakończenia wyprawy


## Zadanie 2
Wykrywaj przypadki rezygnacji ze wspinaczki z powodu zaginięcia (resignation someone missing) dla wypraw, w których bierze udział mniej niż 3 osoby.

Wyniki powinny zawierać, następujące kolumny:
- `peak_name` - nazwa szczytu
- `trip_leader` - imię i nazwisko lidera wyprawy
- `result` - rezultat wyprawy
- `amount_people` - liczba osób biorąca udział w wyprawie

## Zadanie 3
Wykrywaj przypadki rezygnacji ze wspinaczki dla grup, w których liczba osób jest największa w ostatnich 20 wyprawach (batch)

Wyniki powinny zawierać, następujące kolumny:
- `peak_name` - nazwa szczytu
- `trip_leader` - imię i nazwisko lidera wyprawy
- `result` - rezultat wyprawy
- `amount_people` - liczba osób biorąca udział w wyprawie
- `max_people` - maksymalna liczba osób w ostatnich 20 wyprawach
- `ts` - czas rejestracji faktu zakończenia wyprawy

## Zadanie 4
Dla każdego alpinisty (istnieje 10 alpinistów w zbiorze danych) znajdź pierwszą wyprawę, która zakończyła się kontuzją. Następnie dla każdego alpinisty wykrywaj każdą kolejną wyprawę, która zakończyła się kontuzją oraz w wyprawie wzięło udział więcej osób niż w pierwszej nieudanej wyprawie.

Wyniki powinny zawierać, następujące kolumny:
- `first_amount_people` - liczba osób biorąca udział w pierwszej wyprawie
- `first_ts` - czas rejestracji faktu zakończenia pierwszej wyprawy
- `trip_leader` - imię i nazwisko lidera wypraw
- `result` - rezultat wypraw
- `amount_people` - liczba osób biorąca udział w kolejnej wyprawie
- `ts` - czas rejestracji faktu zakończenia kolejnej wyprawy


## Zadanie 5
Wykrywaj co najmniej 4 kolejne wyprawy, które zakończyły się sukcesem w ciągu 1 sekundy.

Wyniki powinny zawierać, następujące kolumny:
- `trip_leader_a` - imię i nazwisko lidera wyprawy a
- `trip_leader_b` - imię i nazwisko lidera wyprawy b
- `trip_leader_c` - imię i nazwisko lidera wyprawy c
- `trip_leader_d` - imię i nazwisko lidera wyprawy d
- `result_a` - rezultat wypraw a
- `result_b` - rezultat wypraw b
- `result_c` - rezultat wypraw c
- `result_d` - rezultat wypraw d
- `ts_a` - czas rejestracji faktu zakończenia wyprawy a
- `ts_b` - czas rejestracji faktu zakończenia wyprawy b
- `ts_c` - czas rejestracji faktu zakończenia wyprawy c
- `ts_d` - czas rejestracji faktu zakończenia wyprawy d


## Zadanie 6
Wykrywaj przypadki liderów wypraw, których wyprawy zakończyły się trzykrotnie niepowodzeniem lub dwukrotnie niepowodzeniem, a ostatnia wypraw udała się.
  W przypadku trzykrotnego niepowodzenia, wykrywaj takie wyprawy, że w kazdej kolejnej brało udział WIĘCEJ osób.
  W przypadku dwukrotnego niepowodzenia, a nastepnie powodzenia wykrywaj wyprawy takie, że:
  a) w DRUGIEJ wyprawie brało udział WIĘCEJ osób niz w PIERWSZEJ
  b) w TRZECIEJ wyprawie brało udział MNIEJ osób niż w PIERWSZEJ

Wyniki powinny zawierać, następujące kolumny:
- `trip_leader` - imię i nazwisko lidera wypraw
- `result_1` - rezultat pierwszej wyprawy
- `result_2` - rezultat drugiej wyprawy
- `result_3a` - rezultat trzeciej wyprawy (może być null jeżeli 3b != null)
- `result_3b` - rezultat trzeciej wyprawy (może być null jeżeli 3a != null)
- `amount_people_1` - liczba osób biorąca udział w pierwszej wyprawie
- `amount_people_2` - liczba osób biorąca udział w drugiej wyprawie
- `amount_people_3a` - liczba osób biorąca udział w trzeciej wyprawie (może być null jeżeli 3b != null)
- `amount_people_3b` - liczba osób biorąca udział w trzeciej wyprawie (może być null jeżeli 3a != null)


## Zadanie 7
Dla każdego lidera wykrywaj takie wyprawy, że:
  a) pierwsze wyprawa się nie udała
  b) od 3-5 kolejnych wypraw udaje się oraz bierze w nich udział mniej osób niż w pierwszej wyprawie
  c) ostatnie wyprawa nie udaje się oraz bierze w niej udział tyle samo lub więcej osób niż w pierwszej wyprawie

Wyniki powinny zawierać, następujące kolumny:
- `trip_leader` - imię i nazwisko lidera wypraw
- `result_a` - rezultat wyprawy a
- `result_b_first` - rezultat pierwszej wyprawy b
- `result_b_last` - rezultat ostatniej wyprawy b
- `result_c` - rezultat wyprawy c
- `amount_people_a` - liczba osób w wyprawie a
- `amount_people_b_first` - liczba osób w pierwszej wyprawie b
- `amount_people_b_last` - liczba osób w ostatniej wyprawie b
- `amount_people_c` - liczba osób w wyprawie c
