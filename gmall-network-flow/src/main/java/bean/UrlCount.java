package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhengyonghong
 * @create 2020--12--19--14:14
 */

@NoArgsConstructor
@AllArgsConstructor
@Data
public class UrlCount {
    private String url;
    private Long windowEnd;
    private Long count;


}
