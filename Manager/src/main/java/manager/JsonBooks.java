package manager;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class JsonBooks {

    private List<Book> books;

    public JsonBooks(List<Book> books) {
        this.books = books;
    }

    public JsonBooks() {
        this.books = new ArrayList<>();
    }

    public List<Book> getBooks() {
        return books;
    }

    public void setBooks(List<Book> books) {
        this.books = books;
    }

    public boolean addBook(Book b) {
        return books.add(b);
    }
}
