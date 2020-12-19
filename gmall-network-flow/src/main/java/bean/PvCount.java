package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhengyonghong
 * @create 2020--12--19--15:46
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PvCount {
    private Long windowEnd;
    private Long count;
}
