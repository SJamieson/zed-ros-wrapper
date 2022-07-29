//
// Created by stewart on 7/28/22.
//

#include <libqoi/qoi.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/CompressedImage.h>
#include <ros/ros.h>

ros::Publisher out;

void callback(sensor_msgs::CompressedImageConstPtr const& compressed_img) {
    if (compressed_img->format != "qoi") throw std::invalid_argument("Compressed image is not QOI encoded!");
    if (compressed_img->data.size() > std::numeric_limits<int>::max()) throw std::runtime_error("Image exceeds maximum QOI decoder size!");
    qoi_desc desc;
    auto const decoded = qoi_decode(compressed_img->data.data(), compressed_img->data.size(), &desc, 0);
    assert(desc.channels == 3 || desc.channels == 4);
    sensor_msgs::Image out_msg;
    size_t const size = desc.channels * desc.width * desc.height;
    out_msg.data.assign(static_cast<uint8_t const* const>(decoded), static_cast<uint8_t const* const>(decoded) + size);
    out_msg.encoding = (desc.channels == 3) ? "8UC3" : "8UC4";
    out_msg.height = desc.height;
    out_msg.width = desc.width;
    out_msg.header = compressed_img->header;
    int const num = 1;
    out_msg.is_bigendian = !(*(char*)&num == 1);
    out_msg.step = desc.width;
    out.publish(out_msg);
}

int main(int argc, char** argv) {
    ros::init(argc, argv, "qoi_republisher");
    ros::NodeHandle nh;
    out = nh.advertise<sensor_msgs::Image>("raw", 1);
    ros::Subscriber input = nh.subscribe<sensor_msgs::CompressedImage>("qoi", 1, callback);
    ros::spin();
    return 0;
}