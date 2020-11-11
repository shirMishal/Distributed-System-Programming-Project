package local_app;

import java.util.Arrays;

public class Book {

    private String title;
    private Review[] reviews;

    public Book(String bookTitle, Review[] reviews) {
        this.title = bookTitle;
        this.reviews = reviews;
    }

    @Override
    public String toString() {
        return "Book [bookTitle=" + title + ", reviews=" + Arrays.toString(reviews) + "]";
    }

}
