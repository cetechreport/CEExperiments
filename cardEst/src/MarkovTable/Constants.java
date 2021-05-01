package MarkovTable;

public class Constants {
    public static final int FORWARD = 1;
    public static final int BACKWARD = 2;

    public static final int OUTGOING = 3;
    public static final int INCOMING = 4;

    // a->b->c OR a->b->c->d
    public static final int ENTRY_TYPE_1 = 0;
    // a->b<-c OR a->b->c<-d
    public static final int ENTRY_TYPE_2 = 1;
    // a<-b->c OR a<-b->c->d
    public static final int ENTRY_TYPE_3 = 2;
    // a<-b->c<-d
    public static final int ENTRY_TYPE_4 = 3;

    public static final int EDGE = 1;
    public static final int VERTEX = 2;
}
