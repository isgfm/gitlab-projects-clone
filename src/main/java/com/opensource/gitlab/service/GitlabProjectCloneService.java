package com.opensource.gitlab.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opensource.gitlab.vo.GitBranch;
import com.opensource.gitlab.vo.GitGroup;
import com.opensource.gitlab.vo.GitProject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 通过gitlab Api自动下载gitLab上的所有项目
 */
@Service
public class GitlabProjectCloneService {

    @Value("${git.gitlabUrl}")
    private String gitlabUrl;

    @Value("${git.privateToken}")
    private String privateToken;

    @Value("${git.projectDir}")
    private String projectDir;
    
    @Value("${git.excluGroupId}")
    private String excluGroupId;
    
    @Value("${git.excluProjectId}")
    private String excluProjectId;
    
    @Value("${git.cloneGroupId}")
    private String cloneGroupId;

    ObjectMapper objectMapper = new ObjectMapper();
    
    private final BlockingQueue<Runnable> blockQueue = new LinkedBlockingQueue<Runnable>(1000);
    
    private final ExecutorService pool = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
            blockQueue, new ThreadPoolExecutor.CallerRunsPolicy());

    @Autowired
    RestTemplate restTemplate;

    @PostConstruct
    private void start() {
        File execDir = new File(projectDir);
        System.out.println("start get gitlab projects");
        List<GitGroup> groups = getGroups();
        List<String> excluGroupIdList = splitBySemi(excluGroupId);
        List<String> excluProjectIdList = splitBySemi(excluProjectId);
        List<String> cloneGroupIdList = splitBySemi(cloneGroupId);
        try {
            System.out.println(objectMapper.writeValueAsString(groups));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        List<GitGroup> filterGroups = filterGroup(groups,cloneGroupIdList,excluGroupIdList);
        for (GitGroup group : filterGroups) {
            String groupId = group.getId().toString();
            List<GitProject> projects = getProjectsByGroup(groupId);
            for (GitProject project : projects) {
                if(excluProjectIdList.contains(project.getId().toString())){
                    continue;
                }
                String lastActivityBranchName = getLastActivityBranchName(project.getId());
                if (StringUtils.isEmpty(lastActivityBranchName)) {
                    System.out.println("branches is empty, break project...");
                    continue;
                }
                clone(lastActivityBranchName, project, execDir);
            }
        }
        System.out.println("end get gitlab projects");
    }

    /**
     * 获取所有项目
     *
     * @return
     */
    private List<GitProject> getAllProjects() {
        String url = gitlabUrl + "/api/v4/projects?per_page={per_page}&private_token={private_token}";
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("per_page", "100");
        uriVariables.put("private_token", privateToken);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity entity = new HttpEntity<>(headers);
        ParameterizedTypeReference<List<GitProject>> responseType = new ParameterizedTypeReference<List<GitProject>>() {
        };
        ResponseEntity<List<GitProject>> responseEntity = restTemplate.exchange(url, HttpMethod.GET, entity, responseType, uriVariables);
        if (HttpStatus.OK == responseEntity.getStatusCode()) {
            return responseEntity.getBody();
        }
        return null;
    }
    
    /**
     * GitlabProjectCloneService
     *
     * Description 根据配置配置要clone哪些group的，排除哪些group的
     * @param groups
     * @param cloneGroupIdList
     * @param excluGroupIdList
     * @return
     * @author guofeiming
     * @date 2022/3/24 14:15
     * @version 1.0.0
     */
    private List<GitGroup> filterGroup(List<GitGroup> groups,List<String> cloneGroupIdList,List<String> excluGroupIdList){
        List<GitGroup> filteredGroup = groups.stream().filter(group->!excluGroupIdList.contains(group.getId().toString())).collect(Collectors.toList());
        if(CollectionUtils.isEmpty(cloneGroupIdList)){
            return filteredGroup;
        }
        return filteredGroup.stream().filter(group->cloneGroupIdList.contains(group.getId().toString())).collect(Collectors.toList());
    }
    
    private List<String> splitBySemi(String str){
        if(StringUtils.isEmpty(str)){
            return Collections.EMPTY_LIST;
        }
        return Arrays.asList(str.split(":"));
    
    }

    /**
     * 获取指定分组下的项目
     *
     * @param group
     * @return
     */
    private List<GitProject> getProjectsByGroup(String group) {
        String url = gitlabUrl + "/api/v4/groups/{group}/projects?per_page={per_page}&private_token={private_token}";
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("group", group);
        uriVariables.put("per_page", "100");
        uriVariables.put("private_token", privateToken);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity entity = new HttpEntity<>(headers);
        ParameterizedTypeReference<List<GitProject>> responseType = new ParameterizedTypeReference<List<GitProject>>() {
        };
        ResponseEntity<List<GitProject>> responseEntity = restTemplate.exchange(url, HttpMethod.GET, entity, responseType, uriVariables);
        if (HttpStatus.OK == responseEntity.getStatusCode()) {
            return responseEntity.getBody();
        }
        return null;
    }

    /**
     * 获取分组列表
     *
     * @return
     */
    private List<GitGroup> getGroups() {
        String url = gitlabUrl + "/api/v4/groups?private_token={private_token}";
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("private_token", privateToken);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity entity = new HttpEntity<>(headers);
        ParameterizedTypeReference<List<GitGroup>> responseType = new ParameterizedTypeReference<List<GitGroup>>() {
        };
        ResponseEntity<List<GitGroup>> responseEntity = restTemplate.exchange(url, HttpMethod.GET, entity, responseType, uriVariables);
        if (HttpStatus.OK == responseEntity.getStatusCode()) {
            return responseEntity.getBody();
        }
        return null;
    }

    /**
     * 获取最近修改的分支名称
     *
     * @param projectId 项目ID
     * @return
     */
    private String getLastActivityBranchName(Long projectId) {
        List<GitBranch> branches = getBranches(projectId);
        if (CollectionUtils.isEmpty(branches)) {
            return "";
        }
        GitBranch gitBranch = getLastActivityBranch(branches);
        return gitBranch.getName();
    }

    /**
     * 获取指定项目的分支列表
     * https://docs.gitlab.com/ee/api/branches.html#branches-api
     *
     * @param projectId 项目ID
     * @return
     */
    private List<GitBranch> getBranches(Long projectId) {
        String url = gitlabUrl + "/api/v4/projects/{projectId}/repository/branches?private_token={privateToken}";
        Map<String, Object> uriVariables = new HashMap<>();
        uriVariables.put("projectId", projectId);
        uriVariables.put("privateToken", privateToken);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity entity = new HttpEntity<>(headers);
        ParameterizedTypeReference<List<GitBranch>> responseType = new ParameterizedTypeReference<List<GitBranch>>() {
        };
        ResponseEntity<List<GitBranch>> responseEntity = restTemplate.exchange(url, HttpMethod.GET, entity, responseType, uriVariables);
        if (HttpStatus.OK == responseEntity.getStatusCode()) {
            return responseEntity.getBody();
        }
        return null;
    }

    /**
     * 获取最近修改的分支
     *
     * @param gitBranches 分支列表
     * @return
     */
    private GitBranch getLastActivityBranch(final List<GitBranch> gitBranches) {
        GitBranch lastActivityBranch = gitBranches.get(0);
        for (GitBranch gitBranch : gitBranches) {
            if (gitBranch.getCommit().getCommittedDate().getTime() > lastActivityBranch.getCommit().getCommittedDate().getTime()) {
                lastActivityBranch = gitBranch;
            }
        }
        return lastActivityBranch;
    }

    private void clone(String branchName, GitProject gitProject, File execDir) {
        String command = String.format("git clone -b %s %s %s", branchName, gitProject.getHttpUrlToRepo(), gitProject.getPathWithNamespace());
        System.out.println("start exec command : " + command);
        try {
            Process exec = Runtime.getRuntime().exec(command, null, execDir);
            cleanStream(exec.getInputStream());
            cleanStream(exec.getErrorStream());
            exec.waitFor();
            System.out.println("================================");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void cleanStream(InputStream stream){
        pool.execute(new Runnable() {
            @Override
            public void run() {
                String line = null;
                try(BufferedReader in = new BufferedReader(new InputStreamReader(stream,"UTF-8"))){
                    while ((line = in.readLine())!=null){
                        System.out.println(line);
                    }
                }catch (IOException e){
                    System.out.println(e.getMessage());
                }
        
            }
        });
    }
}
