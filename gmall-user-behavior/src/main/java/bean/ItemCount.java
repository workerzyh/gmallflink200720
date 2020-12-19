package bean;

/**
 * @author zhengyonghong
 * @create 2020--12--19--10:12
 */
public class ItemCount {
    private Long itemId;
    private Long windowEnd;
    private Long Count;

    public ItemCount() {
    }

    public ItemCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        Count = count;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return Count;
    }

    public void setCount(Long count) {
        Count = count;
    }

    @Override
    public String toString() {
        return "ItemCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", Count=" + Count +
                '}';
    }
}
