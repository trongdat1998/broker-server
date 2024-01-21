/**********************************
 *@项目名称: server-parent
 *@文件名称: io.bhex.broker.server.grpc.server
 *@Date 2018/10/15
 *@Author peiwei.ren@bhex.io 
 *@Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 *注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 ***************************************/
package io.bhex.broker.server.grpc.server;

import com.google.common.base.Strings;
import io.bhex.base.common.EditBaseConfigsRequest;
import io.bhex.base.common.SwtichStatus;
import io.bhex.base.grpc.annotation.GrpcService;
import io.bhex.broker.common.exception.BrokerErrorCode;
import io.bhex.broker.common.exception.BrokerException;
import io.bhex.broker.grpc.app_push.AddAdminPushTaskRequest;
import io.bhex.broker.grpc.app_push.AddAdminPushTaskResponse;
import io.bhex.broker.grpc.app_push.AddPushDeviceRequest;
import io.bhex.broker.grpc.app_push.AddPushDeviceResponse;
import io.bhex.broker.grpc.app_push.AdminPushTaskSimple;
import io.bhex.broker.grpc.app_push.AppPushServiceGrpc;
import io.bhex.broker.grpc.app_push.CancelAdminPushTaskRequest;
import io.bhex.broker.grpc.app_push.CancelAdminPushTaskResponse;
import io.bhex.broker.grpc.app_push.CycleType;
import io.bhex.broker.grpc.app_push.DeleteAdminPushTaskRequest;
import io.bhex.broker.grpc.app_push.DeleteAdminPushTaskResponse;
import io.bhex.broker.grpc.app_push.EditAdminPushTaskRequest;
import io.bhex.broker.grpc.app_push.EditAdminPushTaskResponse;
import io.bhex.broker.grpc.app_push.EditPushSwitchRequest;
import io.bhex.broker.grpc.app_push.EditPushSwitchResponse;
import io.bhex.broker.grpc.app_push.HuweiDeliveryCallbackResponse;
import io.bhex.broker.grpc.app_push.HuweiPushDeliveryCallbackRequest;
import io.bhex.broker.grpc.app_push.PushClickCallbackRequest;
import io.bhex.broker.grpc.app_push.PushClickCallbackResponse;
import io.bhex.broker.grpc.app_push.PushDeliveryCallbackRequest;
import io.bhex.broker.grpc.app_push.PushDeliveryCallbackResponse;
import io.bhex.broker.grpc.app_push.QueryAdminPushTaskRequest;
import io.bhex.broker.grpc.app_push.QueryAdminPushTaskResponse;
import io.bhex.broker.grpc.app_push.QueryAdminPushTaskSendDetailRequest;
import io.bhex.broker.grpc.app_push.QueryAdminPushTaskSendDetailsResponse;
import io.bhex.broker.grpc.app_push.QueryAdminPushTaskSimplesRequest;
import io.bhex.broker.grpc.app_push.QueryAdminPushTaskSimplesResponse;
import io.bhex.broker.grpc.app_push.SendAdminTestPushRequest;
import io.bhex.broker.grpc.app_push.SendAdminTestPushResponse;
import io.bhex.broker.server.domain.BaseConfigConstants;
import io.bhex.broker.server.grpc.server.service.AdminAppPushService;
import io.bhex.broker.server.grpc.server.service.AppPushService;
import io.bhex.broker.server.grpc.server.service.AppPushStatisticsService;
import io.bhex.broker.server.model.AdminPushTask;
import io.bhex.broker.server.model.AdminPushTaskDetail;
import io.bhex.broker.server.model.AdminPushTaskLocale;
import io.bhex.broker.server.model.AdminPushTaskStatistics;
import io.bhex.broker.server.grpc.server.service.BaseConfigService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

@Slf4j
@GrpcService
public class AppPushGrpcService extends AppPushServiceGrpc.AppPushServiceImplBase {

    @Resource
    private AppPushService appPushService;

    @Resource
    private BaseConfigService baseConfigService;

    @Resource
    private AdminAppPushService adminAppPushService;

    @Resource
    private AppPushStatisticsService appPushStatisticsService;

    /**
     * 添加用户推送设备（broker-api调用）
     *
     * @param request
     * @param observer
     */
    @Override
    public void addPushDevice(AddPushDeviceRequest request, StreamObserver<AddPushDeviceResponse> observer) {
        AddPushDeviceResponse response;
        try {
            appPushService.addPushDevice(request.getHeader(), request.getPushDevice());
            response = AddPushDeviceResponse.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (BrokerException e) {
            response = AddPushDeviceResponse.newBuilder().setRet(e.getCode()).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            log.error("addPushDevice error", e);
            observer.onError(e);
        }
    }

    /**
     * 推送送达回报（broker-api调用）
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void pushDeliveryCallback(PushDeliveryCallbackRequest request, StreamObserver<PushDeliveryCallbackResponse> responseObserver) {
        PushDeliveryCallbackResponse response;
        try {
            //TODO
            response = PushDeliveryCallbackResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = PushDeliveryCallbackResponse.newBuilder().setRet(e.getCode()).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("pushDeliveryCallback error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 推送点击回报（broker-api调用）
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void pushClickCallback(PushClickCallbackRequest request, StreamObserver<PushClickCallbackResponse> responseObserver) {
        PushClickCallbackResponse response;
        try {
            log.info("reqOrderId:{},deviceToken:{}", request.getReqOrderId(), request.getDeviceToken());
            appPushStatisticsService.pushClickCallback(request.getReqOrderId(), request.getDeviceToken());
            response = PushClickCallbackResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = PushClickCallbackResponse.newBuilder()
                    .setRet(e.getCode())
                    .setMsg(e.getErrorMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("pushClickCallback error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 发送测试推送(admin使用)
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void sendAdminTestPush(SendAdminTestPushRequest request, StreamObserver<SendAdminTestPushResponse> responseObserver) {
        SendAdminTestPushResponse response;
        try {
            //异步发送不抛异常算成功
            appPushService.sendAdminTestPush(request);
            responseObserver.onNext(SendAdminTestPushResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = SendAdminTestPushResponse.newBuilder()
                    .setRet(e.getCode())
                    .setMsg(e.getErrorMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("sendAdminTestPush error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 添加新的推送任务
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void addAdminPushTask(AddAdminPushTaskRequest request, StreamObserver<AddAdminPushTaskResponse> responseObserver) {
        AddAdminPushTaskResponse response;
        try {
            //判断推送范围
            StringJoiner stringJoiner = new StringJoiner(",");
            if (request.getRangeType() == 9) {
                if (request.getUserIdList().size() > 1000) {
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID, "userIds more than 1000!");
                }
                request.getUserIdList().forEach(userId -> stringJoiner.add(String.valueOf(userId)));
            }
            //判断语言包数量
            if (request.getTaskLocaleList().size() <= 0) {
                throw new BrokerException(BrokerErrorCode.PARAM_INVALID, "userIds at least one!");
            }
            long currentTimeMillis = System.currentTimeMillis();
            long orgId = request.getHeader().getOrgId();
            AdminPushTask adminPushTask = AdminPushTask.builder()
                    .name(request.getName())
                    .orgId(orgId)
                    .pushCategory(request.getPushCategory())
                    .cycleType(request.getCycleType().getNumber())
                    .cycleDayOfWeek(request.getCycleDayOfWeek())
                    .rangeType(request.getRangeType())
                    .userIds(stringJoiner.toString())
                    .firstActionTime(request.getFirstActionTime())
                    .actionTime(request.getFirstActionTime())
                    .expireTime(request.getExpireTime())
                    .defaultLanguage(request.getDefaultLanguage())
                    .created(currentTimeMillis)
                    .updated(currentTimeMillis)
                    .build();
            List<AdminPushTaskLocale> localeList = request.getTaskLocaleList()
                    .stream()
                    .map(pushTaskLocale -> getAdminPushTaskLocale(orgId, currentTimeMillis, pushTaskLocale))
                    .collect(Collectors.toList());
            adminAppPushService.addAdminPushTask(adminPushTask, localeList);
            response = AddAdminPushTaskResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = AddAdminPushTaskResponse.newBuilder()
                    .setRet(e.getCode())
                    .setMsg(e.getErrorMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("addAdminPushTask error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 编辑推送任务（admin使用）
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void editAdminPushTask(EditAdminPushTaskRequest request, StreamObserver<EditAdminPushTaskResponse> responseObserver) {
        EditAdminPushTaskResponse response;
        try {
            StringJoiner stringJoiner = new StringJoiner(",");
            if (request.getRangeType() == 9) {
                if (request.getUserIdList().size() > 1000) {
                    throw new BrokerException(BrokerErrorCode.PARAM_INVALID, "userIds more than 1000!");
                }
                request.getUserIdList().forEach(userId -> stringJoiner.add(String.valueOf(userId)));
            }
            //判断语言包数量
            if (request.getTaskLocaleList().size() <= 0) {
                throw new BrokerException(BrokerErrorCode.PARAM_INVALID, "userIds at least one!");
            }
            long currentTimeMillis = System.currentTimeMillis();
            long orgId = request.getHeader().getOrgId();
            AdminPushTask adminPushTask = AdminPushTask.builder()
                    .taskId(request.getTaskId())
                    .name(request.getName())
                    .orgId(orgId)
                    .pushCategory(request.getPushCategory())
                    .cycleType(request.getCycleType().getNumber())
                    .cycleDayOfWeek(request.getCycleDayOfWeek())
                    .rangeType(request.getRangeType())
                    .userIds(stringJoiner.toString())
                    .firstActionTime(request.getFirstActionTime())
                    .actionTime(request.getFirstActionTime())
                    .expireTime(request.getExpireTime())
                    .defaultLanguage(request.getDefaultLanguage())
                    .updated(currentTimeMillis)
                    .build();
            List<AdminPushTaskLocale> localeList = request.getTaskLocaleList()
                    .stream()
                    .map(pushTaskLocale -> getAdminPushTaskLocale(orgId, currentTimeMillis, pushTaskLocale))
                    .collect(Collectors.toList());
            adminAppPushService.editAdminPushTask(adminPushTask, localeList);
            response = EditAdminPushTaskResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = EditAdminPushTaskResponse.newBuilder()
                    .setRet(e.getCode())
                    .setMsg(e.getErrorMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("editAdminPushTask error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 查询推送任务全量配置(brokerAdmin用于修改)
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void queryAdminPushTask(QueryAdminPushTaskRequest request, StreamObserver<QueryAdminPushTaskResponse> responseObserver) {
        QueryAdminPushTaskResponse response;
        try {
            AdminPushTask adminPushTask = adminAppPushService.queryAdminPushTask(request.getHeader().getOrgId(), request.getTaskId());
            if (adminPushTask == null) {
                throw new BrokerException(BrokerErrorCode.DB_RECORD_NOT_EXISTS);
            }
            List<Long> userIds;
            if (StringUtils.isNotBlank(adminPushTask.getUserIds())) {
                if (adminPushTask.getUserIds().contains(",")) {
                    userIds = Arrays.stream(adminPushTask.getUserIds().split(",")).map(Long::parseLong).collect(Collectors.toList());
                } else {
                    userIds = Collections.singletonList(Long.parseLong(adminPushTask.getUserIds()));
                }
            } else {
                userIds = new ArrayList<>();
            }
            //查找对应的国际化
            List<AdminPushTaskLocale> localeList = adminAppPushService.getPushTaskLocaleList(adminPushTask.getTaskId());
            response = QueryAdminPushTaskResponse.newBuilder()
                    .setTaskId(adminPushTask.getTaskId())
                    .setName(adminPushTask.getName())
                    .setPushCategory(adminPushTask.getPushCategory())
                    .setCycleType(CycleType.forNumber(adminPushTask.getCycleType()))
                    .setCycleDayOfWeek(adminPushTask.getCycleDayOfWeek())
                    .setRangeType(adminPushTask.getRangeType())
                    .addAllUserId(userIds)
                    .setFirstActionTime(adminPushTask.getFirstActionTime())
                    .setExpireTime(adminPushTask.getExpireTime())
                    .setDefaultLanguage(adminPushTask.getDefaultLanguage())
                    .addAllTaskLocale(localeList.stream().map(adminPushTaskLocale -> io.bhex.broker.grpc.app_push.PushTaskLocale
                            .newBuilder()
                            .setLanguage(adminPushTaskLocale.getLanguage())
                            .setPushTitle(adminPushTaskLocale.getPushTitle())
                            .setPushContent(adminPushTaskLocale.getPushContent())
                            .setPushSummary(adminPushTaskLocale.getPushSummary())
                            .setPushUrl(adminPushTaskLocale.getPushUrl())
                            .setUrlType(adminPushTaskLocale.getUrlType())
                            .build())
                            .collect(Collectors.toList())
                    ).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryAdminPushTaskResponse.newBuilder()
                    .setRet(e.getCode())
                    .setMsg(e.getErrorMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryAdminPushTask error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 删除推送任务（admin使用）
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void deleteAdminPushTask(DeleteAdminPushTaskRequest request, StreamObserver<DeleteAdminPushTaskResponse> responseObserver) {
        DeleteAdminPushTaskResponse response;
        try {
            //修改任务状态为cancel
            adminAppPushService.cancelAdminPushTask(request.getHeader().getOrgId(), request.getTaskId(), "user delete!");
            response = DeleteAdminPushTaskResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = DeleteAdminPushTaskResponse.newBuilder()
                    .setRet(e.getCode())
                    .setMsg(e.getErrorMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("deleteAdminPushTask error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 删除推送任务（admin使用）
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void cancelAdminPushTask(CancelAdminPushTaskRequest request, StreamObserver<CancelAdminPushTaskResponse> responseObserver) {
        CancelAdminPushTaskResponse response;
        try {
            //修改任务状态为cancel
            adminAppPushService.cancelAdminPushTask(request.getHeader().getOrgId(), request.getTaskId(), "user cancel!");
            response = CancelAdminPushTaskResponse.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = CancelAdminPushTaskResponse.newBuilder()
                    .setRet(e.getCode())
                    .setMsg(e.getErrorMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("cancelAdminPushTask error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 查询推送任务概要（admin使用）
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void queryAdminPushTaskSimples(QueryAdminPushTaskSimplesRequest request, StreamObserver<QueryAdminPushTaskSimplesResponse> responseObserver) {
        QueryAdminPushTaskSimplesResponse response;
        try {
            if (request.getStartTime() > System.currentTimeMillis()) {
                throw new BrokerException(BrokerErrorCode.PARAM_INVALID, "startTime invalid");
            }
            List<AdminPushTask> taskList = adminAppPushService.queryPushTasks(request.getHeader().getOrgId(), request.getStartTime(), request.getEndTime(), request.getStartId(),
                    request.getEndId(), request.getLimit());
            response = QueryAdminPushTaskSimplesResponse.newBuilder()
                    .addAllTaskSimple(taskList.stream()
                            .map(pushTask -> {
                                //判断程序状态,获取统计数据
                                AdminPushTaskStatistics adminPushTaskStatistics = null;
                                if (pushTask.getStatus() > 0) {
                                    adminPushTaskStatistics = adminAppPushService.queryRecently(pushTask.getTaskId());
                                }
                                AdminPushTaskSimple.Builder builder = AdminPushTaskSimple.newBuilder();
                                if (adminPushTaskStatistics != null) {
                                    builder.setTaskRound(adminPushTaskStatistics.getTaskRound())
                                            .setSendCount(adminPushTaskStatistics.getSendCount())
                                            .setDeliveryCount(adminPushTaskStatistics.getDeliveryCount())
                                            .setClickCount(adminPushTaskStatistics.getClickCount());
                                }
                                return builder.setTaskId(pushTask.getTaskId())
                                        .setName(pushTask.getName())
                                        .setCycleType(pushTask.getCycleType())
                                        .setCycleDayOfWeek(pushTask.getCycleDayOfWeek())
                                        .setFirstActionTime(pushTask.getFirstActionTime())
                                        .setActionTime(pushTask.getActionTime())
                                        .setExpireTime(pushTask.getExpireTime())
                                        .setExecuteTime(pushTask.getExecuteTime())
                                        .setStatus(pushTask.getStatus())
                                        .setRemark(pushTask.getRemark())
                                        .setCreated(pushTask.getCreated())
                                        .setUpdated(pushTask.getUpdated())
                                        .build();
                            }).collect(Collectors.toList())
                    ).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryAdminPushTaskSimplesResponse.newBuilder()
                    .setRet(e.getCode())
                    .setMsg(e.getErrorMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryAdminPushTaskSimples error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 查询推送任务推送详情(broker-amdin)
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void queryAdminPushTaskSendDetail(QueryAdminPushTaskSendDetailRequest request, StreamObserver<QueryAdminPushTaskSendDetailsResponse> responseObserver) {
        QueryAdminPushTaskSendDetailsResponse response;
        try {
            List<AdminPushTaskDetail> taskDetails = adminAppPushService.queryAdminPushTaskSendDetail(request.getHeader().getOrgId(), request.getTaskId(), request.getTaskRound(), request.getStartTime(),
                    request.getLimit());
            response = QueryAdminPushTaskSendDetailsResponse.newBuilder()
                    .addAllDetail(taskDetails.stream()
                            .map(detail -> QueryAdminPushTaskSendDetailsResponse.PushTaskSendDetail.newBuilder()
                                    .setTaskId(detail.getTaskId())
                                    .setTaskRound(detail.getTaskRound())
                                    .setAppId(detail.getAppId())
                                    .setAppChannel(detail.getAppChannel())
                                    .setThirdPushType(detail.getThirdPushType())
                                    .setDeviceType(detail.getDeviceType())
                                    .setLanguage(detail.getLanguage())
                                    .setSendCount(detail.getSendCount())
                                    .setDeliveryCount(detail.getDeliveryCount())
                                    .setClickCount(detail.getClickCount())
                                    .setRemark(detail.getRemark())
                                    .build()
                            ).collect(Collectors.toList()))
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (BrokerException e) {
            response = QueryAdminPushTaskSendDetailsResponse.newBuilder()
                    .setRet(e.getCode())
                    .setMsg(e.getErrorMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryAdminPushTaskSendDetail error", e);
            responseObserver.onError(e);
        }
    }

    /**
     * 华为推送回报（broker-api）
     *
     * @param request
     * @param observer
     */
    @Override
    public void huweiPushDeliveryCallback(HuweiPushDeliveryCallbackRequest request, StreamObserver<HuweiDeliveryCallbackResponse> observer) {
        HuweiDeliveryCallbackResponse response;
        try {
            appPushStatisticsService.huaweiPushDeliveryCallback(request);
            response = HuweiDeliveryCallbackResponse.newBuilder().build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    @Override
    public void editPushSwitch(EditPushSwitchRequest request, StreamObserver<EditPushSwitchResponse> observer) {
        EditPushSwitchResponse response;
        try {

            EditBaseConfigsRequest baseConfigsRequest = EditBaseConfigsRequest.newBuilder()
                    .addConfig(EditBaseConfigsRequest.Config.newBuilder()
                            .setStatus(request.getOpen() ? 1 : 0)
                            .setOrgId(request.getOrgId())
                            .setGroup(BaseConfigConstants.APPPUSH_CONFIG_GROUP)
                            .setKey(request.getSwitchType().name())
                            .setAdminUserName(Strings.nullToEmpty(request.getAdminUserName()))
                            .setSwitchStatus(request.getOpen() ? SwtichStatus.SWITCH_OPEN : SwtichStatus.SWITCH_CLOSE)
                            .build())
                    .build();
            boolean r = baseConfigService.editConfigs(baseConfigsRequest);
            response = EditPushSwitchResponse.newBuilder().setRet(r ? 0 : 1).build();
            observer.onNext(response);
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(e);
        }
    }

    private AdminPushTaskLocale getAdminPushTaskLocale(Long orgId, Long currentTimeMillis, io.bhex.broker.grpc.app_push.PushTaskLocale pushTaskLocale) {
        return AdminPushTaskLocale.builder()
                .language(pushTaskLocale.getLanguage())
                .orgId(orgId)
                .pushContent(pushTaskLocale.getPushContent())
                .pushSummary(pushTaskLocale.getPushSummary())
                .pushTitle(pushTaskLocale.getPushTitle())
                .pushUrl(pushTaskLocale.getPushUrl())
                .urlType(pushTaskLocale.getUrlType())
                .created(currentTimeMillis)
                .updated(currentTimeMillis)
                .build();
    }


}
