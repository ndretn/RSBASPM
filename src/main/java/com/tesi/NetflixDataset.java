package com.tesi;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import java.io.*;
import java.util.Comparator;
import java.util.Scanner;

/**
 * Utility class to manage the not sequential Netflix Dataset
 */
public class NetflixDataset
{
    private Int2ObjectOpenHashMap<ObjectArrayList<Voto>> data;
    private Int2IntOpenHashMap filmData;
    private Int2IntOpenHashMap votiFilmFreq;
    private Int2IntOpenHashMap votiUtenteFreq;

    /**
     * Constructor of the class
    */
     public NetflixDataset()
    {
        data = new Int2ObjectOpenHashMap<>();
        filmData = new Int2IntOpenHashMap();
        votiFilmFreq = new Int2IntOpenHashMap();
        votiUtenteFreq = new Int2IntOpenHashMap();
    }

    /**
     * Adds data to the Netflix Dataset
     * @param file name of the file containing a part of the whole dataset
     */
    public void addData(String file)
    {
        try
        {
            File f = new File(file);
            Scanner sc = new Scanner(f);
            Scanner scl;
            int film = -1;
            short voto;
            int utente;
            int temp;
            short anno;
            short mese;
            short giorno;
            int cont = 0;
            Voto vo;
            String line;
            while (sc.hasNextLine())
            {
                line = sc.nextLine();
                scl = new Scanner(line);
                scl.useDelimiter("[:,-]");
                temp = scl.nextInt();
                if (!scl.hasNextInt())
                {
                    if (film != -1)
                    {
                        filmData.put(film, cont);
                    }
                    cont = 0;
                    film = temp;
                }
                else
                {
                    cont++;
                    utente = temp;
                    voto = scl.nextShort();
                    anno = scl.nextShort();
                    mese = scl.nextShort();
                    giorno = scl.nextShort();
                    scl.close();
                    vo = new Voto(film, voto, giorno, mese, anno);
                    data.putIfAbsent(utente, new ObjectArrayList<>());
                    ObjectArrayList<Voto> current = data.get(utente);
                    current.add(vo);
                }
            }
            filmData.put(film, cont);
            sc.close();
            System.gc();
        }
        catch (FileNotFoundException e)
        {
            System.err.println("File non trovato!\n" + e);
        }
    }

    /**
     * Prints statistics about the Netflix dataset and writes it in a file
     * @param file name of the output file
     */
    public void analizzaDati(String file)
    {
        try
        {
            FileWriter output = new FileWriter(file);
            System.out.println("Numero di utenti: " + data.size());
            output.write("Numero di utenti: " + data.size() + "\n");
            System.out.println("Numero di film: " + filmData.size());
            output.write("Numero di film: " + filmData.size() + "\n");
            int max = 0, min = Integer.MAX_VALUE;
            double somma = 0.0;
            IntCollection film = filmData.values();
            ObjectCollection<ObjectArrayList<Voto>> voti = data.values();
            int numStessaData = 0;
            ObjectCollection<ObjectArrayList<Voto>> vot = data.values();
            for (ObjectArrayList<Voto> v : vot)
            {
                for (int i = 0; i < v.size() - 1; i++)
                {
                    if (v.get(i).compareTo(v.get(i + 1)) == 0) numStessaData++;
                }
            }
            for (ObjectArrayList<Voto> v : voti)
            {
                int numVoti = v.size();
                if (votiUtenteFreq.containsKey(numVoti))
                {
                    int freq = votiUtenteFreq.get(numVoti) + 1;
                    votiUtenteFreq.replace(numVoti, freq);
                }
                else
                {
                    votiUtenteFreq.addTo(numVoti, 1);
                }
                somma += numVoti;
                if (numVoti < min) min = numVoti;
                if (numVoti > max) max = numVoti;
            }
            System.out.println("Numero di voti: " + (int) somma);
            output.write("Numero di voti: " + (int) somma + "\n");
            System.out.println("Numero di voti dati dagli utenti lo stesso giorno: " + numStessaData);
            output.write("Numero di voti dati dagli utenti lo stesso giorno: " + numStessaData + "\n");
            System.out.println("Massimo di voti per utente: " + max);
            output.write("Massimo di voti per utente: " + max + "\n");
            System.out.println("Minimo di voti per utente: " + min);
            output.write("Minimo di voti per utente: " + min + "\n");
            System.out.println("Voti medi per utente: " + somma / (double) data.size());
            output.write("Voti medi per utente: " + somma / (double) data.size() + "\n");
            output.write("Frequenza dei voti per utente: \n");
            for (int j = min; j <= max; j++)
            {
                if (votiUtenteFreq.containsKey(j))
                {
                    output.write(j + " " + votiUtenteFreq.get(j) + "\n");
                }
            }
            max = 0;
            min = Integer.MAX_VALUE;
            somma = 0.0;
            for (int f : film)
            {
                if (votiFilmFreq.containsKey(f))
                {
                    int freq = votiFilmFreq.get(f) + 1;
                    votiFilmFreq.replace(f, freq);
                }
                else
                {
                    votiFilmFreq.addTo(f, 1);
                }
                somma += f;
                if (f < min) min = f;
                if (f > max) max = f;
            }
            System.out.println("Massimo di voti per film: " + max);
            output.write("Massimo di voti per film: " + max + "\n");
            System.out.println("Minimo di voti per film: " + min);
            output.write("Minimo di voti per film: " + min + "\n");
            System.out.println("Voti medi per film: " + somma / (double) filmData.size());
            output.write("Voti medi per film: " + somma / (double) filmData.size() + "\n");
            output.write("Frequenza dei voti per film: \n");
            for (int j = min; j <= max; j++)
            {
                if (votiFilmFreq.containsKey(j))
                {
                    output.write(j + " " + votiFilmFreq.get(j) + "\n");
                }
            }
            output.close();
        }
        catch (IOException e)
        {
            System.err.println("Errore in scrittura! " + e);
        }
    }

    /**
     * Private method to sort the data
     */
    private void order()
    {
        System.out.println("Ordinamento in corso...");
        ObjectCollection<ObjectArrayList<Voto>> d = data.values();
        for (ObjectArrayList<Voto> v : d)
        {
            v.sort(Comparator.naturalOrder());
        }
        System.out.println("Ordinamento concluso!");
    }

    /**
     * Writes the Netflix dataset in the SPMF format
     * @param file the name of the output file
     * @param addValuation true to add the valuation in the item;
     *                     false otherwise
     */
    public void writeSPMFDataset(String file, boolean addValuation)
    {
        try
        {
            PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(file)));
            ObjectCollection<ObjectArrayList<Voto>> d = data.values();
            for (ObjectArrayList<Voto> v : d) {
                v.sort(Comparator.naturalOrder());
                IntArrayList itemset = new IntArrayList();
                if (!addValuation) itemset.add(v.get(0).getFilm());
                else itemset.add(v.get(0).getFilm() * 10 + v.get(0).getValutazione() / 3);
                for (int i = 1; i < v.size(); i++)
                {
                    if (v.get(i - 1).compareTo(v.get(i)) == 0)
                    {
                        if (!addValuation) itemset.add(v.get(i).getFilm());
                        else itemset.add(v.get(i).getFilm() * 10 + v.get(i).getValutazione() / 3);
                    }
                    else if (v.get(i - 1).compareTo(v.get(i)) < 0)
                    {
                        itemset.sort(Comparator.naturalOrder());
                        for (int j = 0; j < itemset.size(); j++) output.write(itemset.getInt(j) + " ");
                        output.write("-1 ");
                        itemset.clear();
                        itemset = new IntArrayList();
                        if (!addValuation) itemset.add(v.get(i).getFilm());
                        else itemset.add(v.get(i).getFilm() * 10 + v.get(i).getValutazione() / 3);
                    }
                    else if (v.get(i - 1).compareTo(v.get(i)) > 0) System.out.println("Error!");
                }
                itemset.sort(Comparator.naturalOrder());
                for (int j = 0; j < itemset.size(); j++) output.write(itemset.getInt(j) + " ");
                output.write("-1 -2\n");
            }
            output.close();
        }
        catch (IOException e)
        {
            System.err.println("Errore in scrittura! " + e);
        }
    }

    /**
     * Inner class to represent the valuation performed by users
    */
    private class Voto implements Comparable<Voto>
    {
        private short valutazione;
        private int film;
        private short giorno;
        private short mese;
        private short anno;

        /**
         * Constructor of the inner class
         * @param film id of the movie
         * @param valutazione value of the rating
         * @param giorno day of the rating
         * @param mese month of the rating
         * @param anno year of the rating
         */
        public Voto(int film, short valutazione, short giorno, short mese, short anno)
        {
            this.film = film;
            this.valutazione = valutazione;
            this.giorno = giorno;
            this.mese = mese;
            this.anno = anno;
        }

        /**
         * Returns the value of the rating
         * @return the rating of the valuation
         */
        public short getValutazione()
        {
            return valutazione;
        }

        /**
         * Returns the id of the movie
         * @return the id of the movie
         */
        public int getFilm()
        {
            return film;
        }

        /**
         * Returns the day of the valuation
         * @return the day of the valuation
         */
        public short getGiorno()
        {
            return giorno;
        }

        /**
         * Returns the month of the valuation
         * @return the month of the valuation
         */
        public short getMese()
        {
            return mese;
        }

        /**
         * Returns the year of the valuation
         * @return the year of the valuation
         */
        public short getAnno()
        {
            return anno;
        }

        /**
         * Compare the date of two ratings
         * @return < 0 if this is supposed to be less than other;
         *         > 0 if this is supposed to be greater than other;
         *         0 if they are supposed to be equal
         */
        @Override
        public int compareTo(Voto other)
        {
            if (anno == other.anno && mese == other.mese && giorno == other.giorno) return 0;
            if (anno < other.anno || (anno == other.anno && mese < other.mese) || (anno == other.anno && mese == other.mese && giorno < other.giorno))
                return -1;
            return 1;

        }
    }
}





