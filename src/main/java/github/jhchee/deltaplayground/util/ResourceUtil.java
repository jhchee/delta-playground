package github.jhchee.deltaplayground.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

public class ResourceUtil {

    public static String readFromFile(String path) {
        InputStream inputStream = ResourceUtil.class.getClassLoader().getResourceAsStream(path);
        assert inputStream != null;
        return new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
    }
}