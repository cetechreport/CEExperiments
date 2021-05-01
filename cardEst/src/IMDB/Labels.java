package IMDB;

public class Labels {
    public static final int NUM_LABELS = 127;
    public static final Integer ALSO_KNOWN_AS = 1;
    public static final Integer IS_KEYWORD_OF = 100;
    public static final Integer IS_EPISODE_OF = 4;

    public static final Integer[] COMPANY_TYPES = new Integer[] {2, 3};

    public static final Integer[] INFO_TYPES = new Integer[] {
        5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
        31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
        51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
        61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
        71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
        81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
        91, 92, 93, 94, 95, 96, 97, 98, 99
    };

    public static final Integer[] LINK_TYPES = new Integer[] {
        101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116
    };

    public static final Integer[] ROLE_TYPES = new Integer[] {
        117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127
    };

    public static final int NO_FILTER = -1;
    public static final int PROD_YEAR_INDEX = 4;
    public static final int MIN_PROD_YEAR = 1880;
    public static final int MAX_PROD_YEAR = 2019;
}
