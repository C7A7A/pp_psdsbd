# Charakterystyka danych
Poszczególne postaci z serialu "Jak poznałem waszą matkę" mówią żarty. 

W strumieniu pojawiają się zdarzenia zgodne ze schematem `JokeEvent`.

```
create json schema JokeEvent(character string, quote string, people_in_room int, laughing_people int, pubs string, ets string, ts string)
```

Każde zdarzenie związane jest z faktem wypowiedzenia żartu, przez określonego bohatera serialu HIMYM. Wydarzeniu temu odpowiada liczba osób znajdujących się w pubie w chwili wypowiedzania żartu, liczba osób, które żart rozbawił oraz nazwa pubu.  

Dane są uzupełnione są o dwie etykiety czasowe. 
* Pierwsza (`ets`) związana jest z momentem wypowiedzenia żartu w rzeczywistości. 
  Etykieta ta może się losowo spóźniać w stosunku do czasu systemowego max do 30 sekund.
* Druga (`its`) związana jest z momentem rejestracji zdarzenia systemie.

# Opis atrybutów

Atrybuty w każdym zdarzeniu zgodnym ze schematem `JokeEvent` mają następujące znaczenie:

* `character` - nazwa postaci opowiadającej żart
* `quote` - cytat
* `people_in_room` - liczba osób w pokoju w chwili wypowiedzenia żartu 
* `laughing_people` - liczba osób, których rozbawił żart
* `pubs` - pub, w którym żart został wypowiedziany
* `ets` - czas wypowiedzenia żartu w rzeczywistości
* `ts` - czas rejestracji w systemie

# Zadania
Opracuj rozwiązania poniższych zadań. 
* Opieraj się strumieniu zdarzeń zgodnych ze schematem `JokeEvent`
* W każdym rozwiązaniu możesz skorzystać z jednego lub kilku poleceń EPL.
* Ostatnie polecenie będące ostatecznym rozwiązaniem zadania musi 
  * być poleceniem `select` 
  * posiadającym etykietę `answer`, przykładowo:
  ```aidl
    @name('answer') SELECT character, laughing_people, sum(people_in_room) as sumPeople_in_room, count(*) howMany, ets, its
    from JokeEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 3 sec)
  ```

## Zadanie 1
Dla każdej osoby generuj średnią ilość osób śmiejących się z jej żartów od początku działania systemu. 

Wyniki powinny zawierać następujące kolumny:
- `character` - nazwa osoby,
- `avglaughing_people` - średnia liczba osób.

## Zadanie 2
Znajduj przypadki wypowiedzenia żartu, z którego nikt się nie zaśmiał. 

Wyniki powinny zawierać następujące kolumny:
- `character` - osoba,
- `its` - czas rejestracji żartu w systemie.

## Zadanie 3
Znajduj zdarzenia, w którym liczba osób śmiejących się, jest największa wśród wszystkich żartów zarejestrowanych w ciągu ostatnich 2 minut. 

Wyniki powinny zawierać następujące kolumny:
- `character` - osoba występująca,
- `laughing_people` - liczba osób, które rozbawił żart,
- `its` - czas rejestracji żartu w systemie.

## Zadanie 4
Chcemy wykrywać małośmieszne puby. Dla każdego pubu i każdej kolejnej minuty, chcemy wyliczać 
- liczbę żartów śmiesznych (liczba osób śmiejących się z żartu jest powyżej 50% osób w pubie)
- liczbę sucharów (liczba osób śmiejących się z żartu jest poniżej 50% osób w pubie).
Znajduj puby, w których w ciągu ostatniej kolejnej minuty liczba sucharów przekroczyła liczbę żartów śmiesznych. 
Wyniki powinny zawierać następujące kolumny:
- `pub` - nazwę pubu,
- `its_start` - czas rozpoczęcia minuty.

## Zadanie 5
Wykrywaj serię żartów w pubie o nazwie McLaren's, podczas których śmiejąca się liczba osób nie wzrastała powyżej 5.
Wyniki powinny zawierać następujące kolumny:
- `pub` - nazwę pubu,
- `its_start` - czas rozpoczęcia serii 

## Zadanie 6
Znajdź następujące po sobie (niekoniecznie bezpośrednio) te trójki żartów, w czasie których liczba osób śmiejących się była większa niż 4. Nie uwzględniaj w wyniku przypadków trójek jeżeli pomiędzy pierwszym a trzecim zdarzem, zanotowano żart wypowiedziany dla publiki liczącej nie mniej niż 30 osób. 
Wyniki powinny zawierać następujące kolumny:
- `laughing_people1` - liczba śmiejących się osób w przypadku zdarzenia pierwszego
- `laughing_people2` - liczba śmiejących się osób w przypadku zdarzenia drugiego
- `laughing_people3` - liczba śmiejących się osób w przypadku zdarzenia trzeciego

## Zadanie 7
Dla każdego pubu, odnajduj serię żartów, dla których liczba osób śmiejących się konsekwentnie malała (co najmniej 4 spadki z rzędu). Seria kończy się przed żartem, w którym liczba osób śmiejących się wreszcie wzrosła.
Wyniki powinny zawierać następujące kolumny:
- `ppl_before` - liczba osób przed spadkami,
- `ppl_after` - liczba osób dla ostatniego żartu w serii spadków,
- `ppl_count` - liczba żartów zanotawanych w serii spadków,
- `pub` - nazwa pubu, w którym zanotowano serię.
