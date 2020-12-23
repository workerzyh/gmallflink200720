package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhengyonghong
 * @create 2020--12--22--15:12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReceiptEvent {
    private String txId;
    private String payChannel;
    private Long timestamp;
}

