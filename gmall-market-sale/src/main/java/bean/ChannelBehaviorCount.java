package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhengyonghong
 * @create 2020--12--21--14:11
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChannelBehaviorCount {
    private String channel;
    private String behavior;
    private String windowEnd;
    private Long count;
}
