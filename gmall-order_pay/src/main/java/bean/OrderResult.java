package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhengyonghong
 * @create 2020--12--22--14:11
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderResult {
    private Long orderId;
    private String eventType;
}
