package manager;

import java.io.Serializable;
import java.util.List;

public class HtmlBook {

    private int color;
    private String url;
    private List<String> entities;
    private boolean isSarcastic;

    public HtmlBook(int color, String url, List<String> entities, boolean isSarcastic) {
        this.color = color;
        this.url = url;
        this.entities = entities;
        this.isSarcastic = isSarcastic;
    }

    public int getColor() {
        return color;
    }

    public String getUrl() {
        return url;
    }

    public List<String> getEntities() {
        return entities;
    }

    public boolean isSarcastic() {
        return isSarcastic;
    }
}
