package manager;

import java.io.Serializable;
import java.util.Arrays;

public class Book {

    private String title;
    private Review[] reviews;

    public Book(String bookTitle, Review[] reviews) {
        this.title = bookTitle;
        this.reviews = reviews;
    }

    public String getBookTitle() {
        return title;
    }

    public void setBookTitle(String bookTitle) {
        this.title = bookTitle;
    }

    public Review[] getReviews() {
        return reviews;
    }

    public void setReviews(Review[] reviews) {
        this.reviews = reviews;
    }

    @Override
    public String toString() {
        return "manager.Book [bookTitle=" + title + ", reviews=" + Arrays.toString(reviews) + "]";
    }
}
