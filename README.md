# pp_psdsbd
semestr 1 - Przetwarzanie strumieni danych w systemach Big Data

Esper - EPL - zestaw zadań 2
Mountain, Mountaineering

Dane dotyczą zdarzeń, które mają miejsce podczas wypraw górskich. Każdy wiersz składa się z nazwy szczytu, lidera wyprawy, rezultatu wyprawy, liczby osób, która bierze udział w wyprawie oraz daty zakończenia wyprawy (timestamp).
peak_name
        typ: string
        znaczenie: nazwa szczytu
        kategoria: atrybut opisujący zdarzenie
trip_leader
        typ: string
        znaczenie: imię i nazwisko lidera wyprawy (znany alpinista/alpinistka)
        kategoria: atrybu opisujący zdarzenie (można też po nim grupować, ale zawiera więcej niż 10 możliwych wartości)
result
        typ: string
        znaczenie: rezultat wyprawy
        kategoria: atrybut po którym można dane grupować (summit reached, base reached, resignation injury, resignation weather, resignation someone    missing, resignation other)
amount_people
        typ: int
        znaczenie: liczba osób biorąca udział w wyprawie
        kategoria: atrybut którego wartości można agregować
ts
        typ: string
        znaczenie: moment zakończenia wyprawy
        kategoria: znacznik czasowy zdarzeń

Opis trzech przykładowych analiz
1. Agregacja
Pokazuj informację jaka jest średnia liczba uczestników wypraw, którym udało się wejść na szczyt lub zdobyć bazę w ciągu ostatnich 10 sekund.
2. Wykrywanie anomalii
Wykrywaj przypadki rezygnacji ze wspinaczki z powodu zaginięcia (resignation someone missing) dla wypraw, w których bierze udział mniej niż 3 osoby.
3. Wykrywanie anomalii opartej na agregacji
Wykrywaj przypadki rezygnacji ze wspinaczki dla grup, w których jest więcej osób niż średnia osób ze wszystkich grup.
