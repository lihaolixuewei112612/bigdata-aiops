package com.apex_ids.udfs;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

public class LabelClassfier {
    public LabelClassfier() {
    }

    public static int getStar(double pc) {
        if (pc > 0.0D && pc <= 0.05D) {
            return 1;
        } else if (pc > 0.05D && pc <= 0.2D) {
            return 2;
        } else {
            return pc > 0.2D ? 3 : 0;
        }
    }

    public static String getPmnlpj(int szpj, int xjpj) {
        StringBuffer pj = new StringBuffer();
        if (szpj == 3) {
            pj.append("您把握大盘上涨能力强");
        } else if (szpj == 2) {
            pj.append("您把握大盘上涨能力佳");
        } else if (szpj == 1) {
            pj.append("您把握大盘上涨能力好");
        } else {
            pj.append("您把握大盘上涨能力弱");
        }

        if (xjpj == 3) {
            pj.append(",规避大盘下跌损失能力强");
        } else if (xjpj == 2) {
            pj.append(",规避大盘下跌损失能力佳");
        } else if (xjpj == 1) {
            pj.append(",规避大盘下跌损失能力好");
        } else {
            pj.append(",规避大盘下跌损失能力弱");
        }

        return pj.toString();
    }

    public static double getYlnlpf(double pc, double syl) {
        double cesyldf = 0.0D;
        double jdsylpf = 0.0D;
        if (pc >= 0.5D) {
            cesyldf = 100.0D;
        } else if (pc <= -0.5D) {
            cesyldf = 0.0D;
        } else {
            cesyldf = (pc + 0.5D) * 100.0D;
        }

        if (syl > 0.5D) {
            jdsylpf = 100.0D;
        } else if (syl > 0.1D && syl <= 0.5D) {
            jdsylpf = 50.0D + syl * 100.0D;
        } else if (syl > 0.0D && syl <= 0.1D) {
            jdsylpf = syl * 100.0D * 6.0D;
        } else {
            jdsylpf = 0.0D;
        }

        return (cesyldf + jdsylpf) / 2.0D;
    }

    public static String getYlnlpj(double pc) {
        if (pc >= 0.5D) {
            return "您这段时间资产增值收益表现完美";
        } else if (pc <= -0.5D) {
            return "您这段时间资产增值收益表现糟糕";
        } else {
            return pc >= 0.0D ? "您这段时间资产增值收益表现良好" : "您这段时间资产增值收益表现不良";
        }
    }

    public static double getFknlpf(double sharp, double zdhcl) {
        double xppf = 0.0D;
        double hcpf = 0.0D;
        if (sharp >= 2.0D) {
            xppf = 100.0D;
        } else if (sharp <= 0.0D) {
            xppf = 0.0D;
        } else {
            xppf = sharp / 0.02D;
        }

        if (zdhcl > 0.5D) {
            hcpf = 0.0D;
        } else {
            hcpf = 100.0D - zdhcl * 2.0D * 100.0D;
        }

        return (xppf + hcpf) / 2.0D;
    }

    public static String getFknlpj(double fknlpf) {
        if (fknlpf > 80.0D) {
            return "总体波动控制强,扛风险能力总体强";
        } else if (fknlpf > 60.0D) {
            return "总体波动控制良好,扛风险能力总体良好";
        } else {
            return fknlpf > 40.0D ? "总体波动控制不佳,扛风险能力总体不佳" : "总体波动控制弱,扛风险能力总体弱";
        }
    }

    public static Double getXgnlpf(double xgcgl) {
        return xgcgl * 100.0D;
    }

    public static String getXgnlpj(double xgcgl) {
        StringBuffer pj = new StringBuffer();
        if (xgcgl > 0.8D) {
            pj.append("您的选股能力很强");
        } else if (xgcgl > 0.6D) {
            pj.append("您的选股能力强");
        } else if (xgcgl > 0.4D) {
            pj.append("您的选股能力不佳");
        } else {
            pj.append("您的选股能力弱");
        }

        return pj.toString();
    }

    public static Double getZsnlpf(double zscgl, double ztcgl, int czcs, int ztcs) {
        if (czcs == 0) {
            return 50.0D;
        } else {
            return ztcs == 0 ? (zscgl + 0.5D) * 100.0D / 2.0D : (zscgl + ztcgl) * 100.0D / 2.0D;
        }
    }

    public static String getZsnlpj(double zscgl, double ztcgl, int czcs, int ztcs) {
        StringBuffer pj = new StringBuffer();
        if (czcs == 0) {
            return "无股票操作";
        } else {
            if (zscgl > 0.8D) {
                pj.append("个股买入价格低于当日收盘价，实出价格高于当日收盘价的择时能力很强");
            } else if (zscgl > 0.6D) {
                pj.append("个股买入价格低于当日收盘价，实出价格高于当日收盘价的择时能力强");
            } else if (zscgl > 0.4D) {
                pj.append("个股买入价格低于当日收盘价，实出价格高于当日收盘价的择时能力不佳");
            } else {
                pj.append("个股买入价格低于当日收盘价，实出价格高于当日收盘价的择时能力弱");
            }

            if (ztcs == 0) {
                return pj.toString();
            } else {
                if (ztcgl > 0.8D) {
                    pj.append(",同一个交易日低买高卖的择时能力很强");
                } else if (ztcgl > 0.6D) {
                    pj.append(",同一个交易日低买高卖的择时能力强");
                } else if (ztcgl > 0.4D) {
                    pj.append(",同一个交易日低买高卖的择时能力不佳");
                } else {
                    pj.append(",同一个交易日低买高卖的择时能力弱");
                }

                return pj.toString();
            }
        }
    }

    public static Double getZhpf(Double yl, Double fk, Double pm, Double zs, Double xg) {
        return yl * 0.3D + fk * 0.2D + pm * 0.2D + zs * 0.15D + xg * 0.15D;
    }

    public static String getCdxcc(Integer dxcg, Integer zxcg, Integer cxcg) {
        if (dxcg >= zxcg && dxcg >= cxcg) {
            return "偏好短线持股";
        } else if (zxcg >= dxcg && zxcg >= cxcg) {
            return "偏好中线持股";
        } else {
            return cxcg >= dxcg && cxcg >= zxcg ? "偏好长线持股" : "空仓";
        }
    }

    public static String getZycjy(Integer zc, Integer yc) {
        if (zc > yc) {
            return "偏好左侧交易";
        } else if (yc > zc) {
            return "偏好右侧交易";
        } else {
            return zc == yc && zc != 0 ? "偏好两侧交易" : "暂无";
        }
    }

    public static String getFxbx(String bdbq, String hcbq) {
        if (bdbq.equals("危险") && hcbq.equals("危险")) {
            return "风险表现激进";
        } else if (!bdbq.equals("危险") && !hcbq.equals("危险")) {
            return !bdbq.equals("均衡") && !hcbq.equals("均衡") ? "风险表现平稳" : "风险表现均衡";
        } else {
            return "风险表现偏激进";
        }
    }

    public static String getFkbq(String bq, Double sz) {
        if (bq.equals("bdl")) {
            if (sz >= 2.0D) {
                return "危险";
            } else {
                return sz >= 1.0D ? "均衡" : "安全";
            }
        } else if (sz >= 0.5D) {
            return "危险";
        } else {
            return sz >= 0.2D ? "均衡" : "安全";
        }
    }

    public static String getDxpph(String tzfg) {
        try {
            String[] strings = tzfg.split(";");
            String[] strings1 = strings[strings.length - 1].split(",");
            String dxp = strings1[0].split(":")[1];
            String lx = strings1[1].split(":")[1];
            return String.format("最爱%s%s风格", dxp, lx);
        } catch (Exception var5) {
            return "暂无风格偏好";
        }
    }

    public static String getHyph(String hyph) {
        try {
            String[] strings = hyph.split(";");
            String[] strings1 = strings[strings.length - 1].split(",");
            String hy = strings1[0].split(":")[1];
            return String.format("最爱%s行业", hy);
        } catch (Exception var4) {
            return "暂无行业偏好";
        }
    }

    public static String getGgph(String ggph) {
        try {
            String[] strings = ggph.split(";");
            String[] strings1 = strings[strings.length - 1].split(",");
            String mc = "";

            try {
                mc = strings1[2].split(":")[1];
            } catch (Exception var5) {
                mc = strings1[1].split(":")[1];
            }

            return String.format("最爱%s", mc);
        } catch (Exception var6) {
            return "暂无个股偏好";
        }
    }

    public static String getCzfgZhpj(String cdx, String zyc, String fxbx, String dxpph) {
        String trency = "";
        String positive = "";
        if (cdx.contains("长线") && (dxpph.contains("大盘") || dxpph.contains("中盘")) && dxpph.contains("价值") && !zyc.contains("右侧")) {
            trency = "价值型";
        } else if (cdx.contains("中线") && !zyc.contains("右侧")) {
            trency = "趋势型";
        } else {
            trency = "投机型";
        }

        if (fxbx.contains("平稳") && !zyc.contains("右侧")) {
            positive = "较好";
        } else if (fxbx.contains("危险") && zyc.contains("右侧")) {
            positive = "较弱";
        } else {
            positive = "正常";
        }

        return String.format("您属于做%s投资者,操作具有%s的进攻防御意识", trency, positive);
    }
}

