package local_app;

import java.util.ArrayList;
import java.util.List;

public class JsonBooks {
    private List<Book> books;


    public JsonBooks() {
        this.books = new ArrayList<>();
    }

    public boolean addBook(Book b) {
        return books.add(b);
    }
}
