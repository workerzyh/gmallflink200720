package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhengyonghong
 * @create 2020--12--21--15:48
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BlackListWarning {
    private Long userId;
    private Long adId;
    private String warningMsg;
}

