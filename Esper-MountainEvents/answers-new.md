# Zadania
## Zadanie 1
Dla każdego rezultatu wypraw zarejestrowanych w ciągu ostatnich 10 sekund utrzymuj liczbę osób, która brała w nich udział.

Wyniki powinny zawierać, następujące kolumny:
- `result` - rezultat wyprawy
- `sum_people` - suma liczby osób

## Odpowiedź
select result, sum(amount_people) as sum_people 
from MountainEvent(result IN ('summit-reached', 'base-reached'))#time(10) 
group by result;

## Zadanie 2
Wykrywaj przypadki końca wypraw z powodu zaginięcia (resignation someone missing), ogranicz analizę tylko do wypraw, w których brało udział mniej niż 3 osoby.

Wyniki powinny zawierać, następujące kolumny:
- `peak_name` - nazwa szczytu
- `trip_leader` - imię i nazwisko lidera wyprawy
- `result` - rezultat wyprawy
- `amount_people` - liczba osób biorąca udział w wyprawie

## Odpowiedź
select peak_name, trip_leader, result, amount_people
from MountainEvent(result = "resignation-someone-missing")
where amount_people < 3;

## Zadanie 3
Analizując jedynie wyprawy zakończone z powodu zaginięcia (resignation someone missing) wykrywaj takie, w których liczba osób jest największa z ostatnich 20-tu wypraw.

Wyniki powinny zawierać, następujące kolumny:
- `peak_name` - nazwa szczytu
- `trip_leader` - imię i nazwisko lidera wyprawy
- `result` - rezultat wyprawy
- `amount_people` - liczba osób biorąca udział w wyprawie
- `max_people` - maksymalna liczba osób w ostatnich 20 wyprawach
- `its` - czas rejestracji faktu zakończenia wyprawy

## Odpowiedź
select peak_name, trip_leader, result, amount_people, max(amount_people) as max_people, its
from MountainEvent(result = "resignation-someone-missing")#length(20)
having max(amount_people) = amount_people;

## Zadanie 4
Dla każdych kolejnych 5 sekund wyznaczana jest lista z nazwami 10 szczytów, które były celem największej liczby wypraw. Znajduj nazwy takich szczytów, które zostały usunięte z powyższej listy.

Wyniki powinny zawierać, następujące kolumny:
- `peak_name` - nazwa szczytu
- `how_many` - liczba wypraw jaka miała na celu szczyt przed jego usunięcem z listy

## Odpowiedź

## Zadanie 5
Wykrywaj serię nie dłuższą niż 5 sekund i składającą się z co najmniej 3 kolejnych wypraw zakończonych sukcesem (summit reached, base reached), po której to serii miała miejsce wyprawa o efekcie innym niż sukces.

Wyniki powinny zawierać, następujące kolumny:
- `result_1` - rezultat pierwszej wyprawy w serii
- `result_2` - rezultat drugiej wyprawy w serii
- `result_3` - rezultat trzeciej wyprawy w serii
- `its_1` - czas rejestracji faktu zakończenia pierwszej wyprawy w serii
- `its_2` - czas rejestracji faktu zakończenia drugiej wyprawy w serii
- `its_3` - czas rejestracji faktu zakończenia trzeciej wyprawy w serii


## Odpowiedź
select a[0].its as its_1, a[1].its as its_2, a[2].its as its_3,
a[0].result as result_1, a[1].result as result_2, a[2].result as result_3
from pattern [
    every ([3:] a=MountainEvent(result in ("summit-reached", "base-reached"))
        until b=MountainEvent(result in ("resignation-injury", "resignation-weather", "resignation-someone-missing", "resignation-other"))
        where timer:within(5 sec)
    )
]
## Zadanie 6
Wykrywaj przypadki liderów wypraw, których kolejne wyprawy zakończyły się trzykrotnie niepowodzeniem lub trzykrotnie powodzeniem.
Zadbaj o to aby wykluczyć przypadki, w których pomiędzy pierwszą wyprawą a trzecią nastąpiły wyprawy innego lidera.

Wyniki powinny zawierać, następujące kolumny:
- `trip_leader` - imię i nazwisko lidera wypraw
- `result_1` - rezultat pierwszej wyprawy
- `result_2` - rezultat drugiej wyprawy
- `result_3` - rezultat trzeciej wyprawy

## Odpowiedź

## Zadanie 7
Dla każdego lidera wykrywaj takie wyprawy, że:
a) pierwsza wyprawa się nie udała
b) od 3-5 kolejnych wypraw zakończyło się sukcesem (summit reached, base reached) i w kazdej z nich brało udział mniej osób niż w pierwszej wyprawie
c) ostatnia wyprawa zakończyła się porażką i brało w niej udział tyle samo lub więcej osób co w pierwszej wyprawie

Wyniki powinny zawierać, następujące kolumny:
- `its_a` - czas rejestracji faktu zakończenia wyprawy a
- `trip_leader` - imię i nazwisko lidera wypraw
- `result_a` - rezultat wyprawy a
- `result_b_first` - rezultat pierwszej wyprawy b
- `result_c` - rezultat wyprawy c
- `amount_people_a` - liczba osób w wyprawie a
- `amount_people_b_first` - liczba osób w pierwszej wyprawie b
- `amount_people_c` - liczba osób w wyprawie c

## Odpowiedź
select * from MountainEvent
    match_recognize (
    partition by trip_leader
    measures
        A.its as its_a, A.trip_leader as trip_leader, A.result as result_a, A.amount_people as amount_people_a,
        B[0].result as result_b_first, B[0].amount_people as amount_people_b_first,
        C.result as result_c, C.amount_people as amount_people_c
    pattern (A B{3,5} C)
    define
        A as A.result not in ("summit-reached", "base-reached"),
        B as B.amount_people < A.amount_people and B.result in ("summit-reached", "base-reached"),
        C as C.amount_people >= A.amount_people and C.result not in ("summit-reached", "base-reached")
    )