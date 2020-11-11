package manager;

import java.io.Serializable;

public class Review {


    //fields
    private String id;
    private String link;
    private String title;
    private String text;
    private int rating;
    private String author;
    private String date;

    //constructor
    public Review(String id, String link, String reviewTitle, String reviewText, int rating, String outhor, String date) {
        this.id = id;
        this.link = link;
        this.title = reviewTitle;
        this.text = reviewText;
        this.rating = rating;
        this.author = outhor;
        this.date = date;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getReviewTitle() {
        return title;
    }

    public void setReviewTitle(String reviewTitle) {
        this.title = reviewTitle;
    }

    public String getReviewText() {
        return text;
    }

    public void setReviewText(String reviewText) {
        this.text = reviewText;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    public String getOuthor() {
        return author;
    }

    public void setOuthor(String outhor) {
        this.author = outhor;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "manager.Review [id=" + id + ", link=" + link + ", reviewTitle=" + title + ", reviewText=" + text
                + ", rating=" + rating + ", outhor=" + author + ", date=" + date + "]";
    }

}
