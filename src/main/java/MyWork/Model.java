package MyWork;

import java.util.List;

/**
 * Created by cube on 16-11-15.
 */
public class Model {
}

class Person {
    public Person(String name, int age,String birthday, List<Book> books) {
        this.name = name;
        this.age = age;
        this.books=books;
        this.birthday=birthday;
    }
    public Person(String name, int age, List<Book> books) {
        this.name = name;
        this.age = age;
        this.books=books;
    }

    private int age;
    private String name;
    private List<Book> books;
    private String birthday;

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Book> getBooks() {
        return books;
    }

    public void setBooks(List<Book> books) {
        this.books = books;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }
}

class Book {
    public Book(String name, int price) {
        this.name = name;
        this.price = price;
    }

    private int price;
    private String name;

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

