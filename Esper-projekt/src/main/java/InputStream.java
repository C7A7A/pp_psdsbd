import com.espertech.esper.runtime.client.EPEventService;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class InputStream {

    private static final String DATA_ROZPOCZECIA = "2001-09-05";
    private static final String DATA_ZAKONCZENIA = "2001-09-20";

    private static final int LICZBA_PLIKOW = 12;
    private static final int MAX_LICZBA_BLEDOW = 5;
    private static InformacjeOPliku[] tablicaInformacjiOPlikach = new InformacjeOPliku[LICZBA_PLIKOW];
    private static DateFormat df = null;

    public void generuj(EPEventService eventService) throws IOException {
        tablicaInformacjiOPlikach[0] = new InformacjeOPliku(
                "files/tableAPPLE_NASDAQ.csv", "Apple", "NASDAQ");
        tablicaInformacjiOPlikach[1] = new InformacjeOPliku(
                "files/tableCOCACOLA_NYSE.csv", "CocaCola", "NYSE");
        tablicaInformacjiOPlikach[2] = new InformacjeOPliku(
                "files/tableDISNEY_NYSE.csv", "Disney", "NYSE");
        tablicaInformacjiOPlikach[3] = new InformacjeOPliku(
                "files/tableFORD_NYSE.csv", "Ford", "NYSE");
        tablicaInformacjiOPlikach[4] = new InformacjeOPliku(
                "files/tableGOOGLE_NASDAQ.csv", "Google", "NASDAQ");
        tablicaInformacjiOPlikach[5] = new InformacjeOPliku(
                "files/tableHONDA_NYSE.csv", "Honda", "NYSE");
        tablicaInformacjiOPlikach[6] = new InformacjeOPliku(
                "files/tableIBM_NASDAQ.csv", "IBM", "NASDAQ");
        tablicaInformacjiOPlikach[7] = new InformacjeOPliku(
                "files/tableINTEL_NASDAQ.csv", "Intel", "NASDAQ");
        tablicaInformacjiOPlikach[8] = new InformacjeOPliku(
                "files/tableMICROSOFT_NASDAQ.csv", "Microsoft", "NASDAQ");
        tablicaInformacjiOPlikach[9] = new InformacjeOPliku(
                "files/tableORACLE_NASDAQ.csv", "Oracle", "NASDAQ");
        tablicaInformacjiOPlikach[10] = new InformacjeOPliku(
                "files/tablePEPSICO_NYSE.csv", "PepsiCo", "NYSE");
        tablicaInformacjiOPlikach[11] = new InformacjeOPliku(
                "files/tableYAHOO_NASDAQ.csv", "Yahoo", "NASDAQ");

        ReverseLineReader[] readers = new ReverseLineReader[LICZBA_PLIKOW];

        try {
            for (int i = 0; i < LICZBA_PLIKOW; i++) {
                readers[i] = new ReverseLineReader(new File(
                        tablicaInformacjiOPlikach[i].getNazwaPliku()), "UTF-8");
            }
        } catch (FileNotFoundException e) {
            System.err.println("Nie odnaleziono pliku");
            System.exit(1);
        }

        Date dataRozpoczecia = null;
        Date dataZakonczenia = null;


        try {
            df = new SimpleDateFormat("yyyy-MM-dd");
            dataRozpoczecia = df.parse(DATA_ROZPOCZECIA);
            dataZakonczenia = df.parse(DATA_ZAKONCZENIA);
        } catch (ParseException e) {
            System.err.println("Nie udalo się wczytac podanych dat rozpoczecia i zakonczenia!");
            System.exit(1);
        }

        String[] linie = new String[LICZBA_PLIKOW];

        // Przesuniecie do pierwszych notowan z zakresu dat

        String[] splitResult = null;
        for (int i = 0; i < LICZBA_PLIKOW; i++) {
            while ((linie[i] = readers[i].readLine()) != null) {
                splitResult = linie[i].split(",");
                Date dataNotowania = null;
                try {
                    dataNotowania = df.parse(splitResult[0]);
                    if (dataNotowania.compareTo(dataRozpoczecia) >= 0) {
                        break;
                    }
                } catch (Exception e) {
                }
            }
        }

        Date iteratorDaty = dataRozpoczecia;

        // Glowna petla

        int liczbaBledow = 0;
        while ((iteratorDaty.compareTo(dataZakonczenia) <= 0)
                && (liczbaBledow < MAX_LICZBA_BLEDOW)) {

            for (int i = 0; i < LICZBA_PLIKOW; i++) {
                try {
                    Date dataNotowania = wyodrebnijDate(linie[i]);

                    if (dataNotowania == null) {
                        continue;
                    }

                    if ((dataNotowania.compareTo(iteratorDaty) == -1)) {
                        // Data ostatnio wczytanego notowania wczesniejsza niz
                        // biezaca data - pobierz kolejne notowanie!
                        if ((linie[i] = readers[i].readLine()) != null) {
                            dataNotowania = wyodrebnijDate(linie[i]);
                        }
                    } else if ((dataNotowania.compareTo(iteratorDaty) == 1)) {
                        // Data ostatnio wczytanego notowania poniejsza niz
                        // biezaca data - czekaj!
                        continue;
                    }

                    if ((dataNotowania != null) && (dataNotowania.equals(iteratorDaty))) {
                        // Tworzenie obiektu notowania
                        splitResult = linie[i].split(",");
                        KursAkcji kurs = new KursAkcji(
                                tablicaInformacjiOPlikach[i].getNazwaSpolki(),
                                tablicaInformacjiOPlikach[i].getNazwaMarketu(),
                                dataNotowania,
                                Float.valueOf(splitResult[1].trim()),
                                Float.valueOf(splitResult[2].trim()),
                                Float.valueOf(splitResult[3].trim()),
                                Float.valueOf(splitResult[4].trim()),
                                Float.valueOf(splitResult[5].trim()));
//                        System.out.println(kurs.toString());
                        eventService.sendEventBean(kurs, kurs.getClass().getName());
                    }
                } catch (Exception e) {
                    liczbaBledow++;
                    System.err.println("Blad parsowania! [" + linie[i] + "]. Po raz: " + liczbaBledow);

                    if (liczbaBledow >= MAX_LICZBA_BLEDOW) {
                        System.err.println("Za duzo bledow!");
                        break;
                    }
                }
            }

            // Inkrementacja daty
            iteratorDaty = inkrementujDate(iteratorDaty);
        }
    }

    // Metody pomocnicze
    private static Date inkrementujDate(Date data) {
        Calendar c = Calendar.getInstance();
        c.setTime(data);
        c.add(Calendar.DATE, 1);
        return c.getTime();
    }

    private static Date wyodrebnijDate(String linia) {
        String[] splitResult = linia.split(",");
        if (!splitResult[0].equals("Date")) {
            try {
                return df.parse(splitResult[0]);
            } catch (ParseException e) {
            }
        }
        return null;
    }

    // Klasa pomocnicza
    private static class InformacjeOPliku {
        private String nazwaPliku;
        private String nazwaSpolki;
        private String nazwaMarketu;

        public InformacjeOPliku(String nazwaPliku, String nazwaSpolki, String nazwaMarketu) {
            this.nazwaPliku = nazwaPliku;
            this.nazwaSpolki = nazwaSpolki;
            this.nazwaMarketu = nazwaMarketu;
        }

        public String getNazwaPliku() {
            return nazwaPliku;
        }

        public String getNazwaSpolki() {
            return nazwaSpolki;
        }

        public String getNazwaMarketu() {
            return nazwaMarketu;
        }
    }

    // Klasa pomocnicza
    // AUTOR: WhiteFang34
    // ZRODO: http://stackoverflow.com/questions/6011345/read-a-file-line-by-line-in-reverse-order
    private static class ReverseLineReader {
        private static final int BUFFER_SIZE = 8192;

        private final FileChannel channel;
        private final String encoding;
        private long filePos;
        private ByteBuffer buf;
        private int bufPos;
        private byte lastLineBreak = '\n';
        private ByteArrayOutputStream baos = new ByteArrayOutputStream();

        public ReverseLineReader(File file, String encoding) throws IOException {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            channel = raf.getChannel();
            filePos = raf.length();
            this.encoding = encoding;
        }

        public String readLine() throws IOException {
            while (true) {
                if (bufPos < 0) {
                    if (filePos == 0) {
                        if (baos == null) {
                            return null;
                        }
                        String line = bufToString();
                        baos = null;
                        return line;
                    }

                    long start = Math.max(filePos - BUFFER_SIZE, 0);
                    long end = filePos;
                    long len = end - start;

                    buf = channel.map(FileChannel.MapMode.READ_ONLY, start, len);
                    bufPos = (int) len;
                    filePos = start;
                }

                while (bufPos-- > 0) {
                    byte c = buf.get(bufPos);
                    if (c == '\r' || c == '\n') {
                        if (c != lastLineBreak) {
                            lastLineBreak = c;
                            continue;
                        }
                        lastLineBreak = c;
                        return bufToString();
                    }
                    baos.write(c);
                }
            }
        }

        private String bufToString() throws UnsupportedEncodingException {
            if (baos.size() == 0) {
                return "";
            }

            byte[] bytes = baos.toByteArray();
            for (int i = 0; i < bytes.length / 2; i++) {
                byte t = bytes[i];
                bytes[i] = bytes[bytes.length - i - 1];
                bytes[bytes.length - i - 1] = t;
            }

            baos.reset();

            return new String(bytes, encoding);
        }
    }

}
