package com.jilani.breadsdataloader;

import com.jilani.breadsdataloader.author.Author;
import com.jilani.breadsdataloader.author.AuthorRepository;
import com.jilani.breadsdataloader.book.Book;
import com.jilani.breadsdataloader.book.BookRepository;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private BookRepository bookRepository;

    public static void main(String[] args) {
        SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

    @PostConstruct
    private void start() {
        //initAuthors();
        initWorks();
    }

    private void initAuthors() {
        Path PATH = Paths.get(authorDumpLocation);
        System.out.println("Openning File...");
        try(Scanner scanner = new Scanner(Files.newBufferedReader(PATH))){
            while(scanner.hasNextLine()) {
                //read line
                String line = scanner.nextLine();

                //parse line and get json string of author
                String authorJsonString = line.substring(line.indexOf("{"));
                if(StringUtils.hasText(authorJsonString)) {
                    try {
                        JSONObject authorJson = new JSONObject(authorJsonString);
                        String name = authorJson.optString("name");
                        String personalName = authorJson.optString("personal_name");
                        String id = authorJson.getString("key").replace("/authors/","");

                        //Construct author object
                        Author author = new Author();
                        author.setId(id);
                        author.setName(name);
                        author.setPersonalName(personalName);

                        System.out.println("Saving author " + id + ": " + name + "\t" + personalName + "...");
                        //persist author in db
                        authorRepository.save(author);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private void initWorks() {
        Path PATH = Paths.get(worksDumpLocation);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        System.out.println("Openning WorksDump File...");
        try(Scanner scanner = new Scanner(Files.newBufferedReader(PATH))){
            while(scanner.hasNextLine()) {
                //read line
                String line = scanner.nextLine();

                //parse line and get json string of book
                String workJsonString = line.substring(line.indexOf("{"));
                if(StringUtils.hasText(workJsonString)) {
                    try {
                        JSONObject workJson = new JSONObject(workJsonString);

                        //Construct book object
                        Book book = new Book();

                        String bookName = workJson.optString("title");
                        book.setName(bookName);

                        book.setId(workJson.getString("key").replace("/works/",""));

                        String description = "";
                        JSONObject jsonObject = workJson.optJSONObject("description");
                        if(jsonObject != null) {
                            description = jsonObject.optString("value");
                            book.setDescription(description);
                        }

                        LocalDate publishedDate = null;
                        jsonObject = workJson.optJSONObject("created");
                        if(jsonObject != null) {
                            publishedDate = LocalDate.parse(jsonObject.getString("value"),dateTimeFormatter);
                            book.setPublishedDate(publishedDate);
                        }

                        JSONArray coversJSONArr = workJson.optJSONArray("covers");
                        if(coversJSONArr != null) {
                            List<String> coverIds = new ArrayList<>();
                            for (int i = 0; i < coversJSONArr.length() ; i++) {
                                coverIds.add(coversJSONArr.getString(i));
                            }
                            book.setCoverIds(coverIds);
                        }

                        JSONArray authorsJSONArr = workJson.optJSONArray("authors");
                        if(authorsJSONArr != null) {
                            List<String> authorIds = new ArrayList<>();
                            List<String> authorNames = new ArrayList<>();
                            for (int i = 0; i < authorsJSONArr.length() ; i++) {
                                String authorId = authorsJSONArr.getJSONObject(i).
                                        getJSONObject("author").getString("key").replace("/authors/","");
                                authorIds.add(authorId);

                                Optional<Author> optionalAuthor = authorRepository.findById(authorId);
                                if(optionalAuthor.isPresent()){
                                    authorNames.add(optionalAuthor.get().getName());
                                } else
                                    authorNames.add("Unknown Author");
                            }
                            book.setAuthorIds(authorIds);
                            book.setAuthorNames(authorNames);
                        }

                        //persist book in db
                        bookRepository.save(book);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }
}
