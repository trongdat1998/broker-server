package io.bhex.broker.server.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "tb_red_packet_token_config")
public class RedPacketTokenConfig {

    @Id
    @GeneratedValue(generator = "JDBC")
    private Long id;
    private Long orgId;
    private String tokenId;
    private String tokenName;
    private BigDecimal minAmount; // sing redPacket minPrecision and minAmount
    private BigDecimal maxAmount; // sing redPacket maxAmount
    private Integer maxCount;
    private BigDecimal maxTotalAmount;
    private Integer status;
    private Integer customOrder;
    private Long created;
    private Long updated;

    public io.bhex.broker.grpc.red_packet.RedPacketTokenConfig convertGrpcObj() {
        return io.bhex.broker.grpc.red_packet.RedPacketTokenConfig.newBuilder()
                .setId(this.id)
                .setOrgId(this.orgId)
                .setTokenId(this.tokenId)
                .setTokenName(this.tokenName)
                .setMinAmount(this.minAmount.stripTrailingZeros().toPlainString())
                .setMaxAmount(this.maxAmount.stripTrailingZeros().toPlainString())
                .setMaxCount(this.maxCount)
                .setMaxTotalAmount(this.maxTotalAmount.stripTrailingZeros().toPlainString())
                .setStatus(this.status)
                .setCustomIndex(this.customOrder)
                .build();
    }

//    private static String[] getRandomAmount(BigDecimal totalAmount, Integer totalCount, RedPacketTokenConfig redPacketTokenConfig) {
//        Integer remainCount = totalCount;
//        BigDecimal remainAmount = totalAmount;
//        String[] amountAssignInfo = new String[totalCount];
//        int scale = redPacketTokenConfig.getMinAmount().scale();
//        Random random = new Random();
//        while (remainCount > 0) {
//            BigDecimal amount;
//            if (remainCount == 1) {
//                amount = remainAmount;
//            } else {
//                amount = BigDecimal.valueOf(random.nextDouble() *
//                        (remainAmount.divide(new BigDecimal(remainCount), scale, RoundingMode.DOWN).multiply(new BigDecimal("2"))).doubleValue())
//                        .setScale(scale, RoundingMode.DOWN);
//                if (amount.compareTo(redPacketTokenConfig.getMinAmount()) < 0) {
//                    amount = redPacketTokenConfig.getMinAmount();
//                }
//            }
//            remainCount--;
//            remainAmount = remainAmount.subtract(amount);
//            amountAssignInfo[remainCount] = amount.stripTrailingZeros().toPlainString();
//        }
//        return amountAssignInfo;
//    }
//
//    public static void main(String[] args) {
//        RedPacketTokenConfig config = RedPacketTokenConfig.builder()
//                .minAmount(new BigDecimal("0.01"))
//                .build();
//        BigDecimal[] totals = new BigDecimal[]{new BigDecimal("1"), new BigDecimal("11"), new BigDecimal("13"), new BigDecimal("3"), new BigDecimal("9")};
//        Random random = new Random();
//        for (int index = 0; index < 5; index++) {
//            int count = random.nextInt(20) + 5;
//            System.out.println(String.format("total:%s, count:%s", totals[index].stripTrailingZeros().toPlainString(), count));
//            String[] arrays = getRandomAmount(totals[index], count, config);
//            System.out.println(String.format("arrays %s sum = %s", Arrays.toString(arrays), Stream.of(arrays).map(BigDecimal::new).reduce(BigDecimal::add).orElse(BigDecimal.ZERO).stripTrailingZeros().toPlainString()));
//        }
//        for (int index = 0; index < 100; index++) {
//            String[] arrays = getRandomAmount(new BigDecimal(100), 5, config);
//            System.out.println(String.format("arrays %s sum = %s", Arrays.toString(arrays), Stream.of(arrays).map(BigDecimal::new).reduce(BigDecimal::add).orElse(BigDecimal.ZERO).stripTrailingZeros().toPlainString()));
//
//        }
//    }

}
