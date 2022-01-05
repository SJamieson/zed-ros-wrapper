//
// Created by stewart on 12/28/21.
//

#ifndef ros_marathon_example_ROS_MARATHON_HPP
#define ros_marathon_example_ROS_MARATHON_HPP

#include <ros/ros.h>
#include <ros/serialization.h>
#include <ros/file_log.h>
#include "marathon.hpp"
#include <ros_marathon_example/MarathonMsg.h>
#include <image_transport/camera_common.h>

namespace ros_marathon {
    template <class M>
    class use_marathon_transport : public std::false_type {};

#define USE_MARATHON_TRANSPORT(Class) template <> class use_marathon_transport<Class> : public std::true_type {};

    class MarathonTransport;
    class CameraPublisher;

    std::string sanitize(std::string topic_name) {
        std::replace(topic_name.begin(), topic_name.end(), '/', '-');
        return topic_name;
    }

    template <typename MessageType>
    class Publisher {
        friend class MarathonTransport;
        friend class CameraPublisher;
        ros::Publisher pub;
        ros::Publisher marathon_pub;
        Marathon::TableArchive<MessageType, ros::serialization::Serializer> archive;

        Publisher(ros::NodeHandle& nh, std::string const& topic, uint32_t const& buffer, bool const& latch = false)
            : pub(nh.advertise<MessageType>(topic, buffer, latch))
            , marathon_pub(nh.advertise<ros_marathon_example::MarathonMsg>(topic + "/marathon", buffer, latch))
            , archive(ros::file_log::getLogDirectory(), sanitize(topic), Marathon::ModeType::READ_APPEND, 32 * 1024 * 1024) {}

    public:
        void publish(MessageType const& msg) {
            auto const id = archive.write(msg);
            if (pub.getNumSubscribers()) pub.publish(msg);
            if (marathon_pub.getNumSubscribers()) {
                ros_marathon_example::MarathonMsg mmsg;
                mmsg.archive_id = archive.get_name();
                mmsg.row = id;
                marathon_pub.publish(mmsg);
            }
        }

        void publish(boost::shared_ptr<MessageType> const& msg) {
            if (pub.getNumSubscribers()) pub.publish(msg);
            auto const id = archive.write(*msg);
            if (marathon_pub.getNumSubscribers()) {
                ros_marathon_example::MarathonMsg mmsg;
                mmsg.archive_id = archive.get_name();
                mmsg.row = id;
                marathon_pub.publish(mmsg);
            }
        }

        uint32_t getNumSubscribers() const {
            return pub.getNumSubscribers() + marathon_pub.getNumSubscribers();
        }
    };

    class CameraPublisher {
        friend class MarathonTransport;
        Publisher<sensor_msgs::CameraInfo> infoPub;
        Publisher<sensor_msgs::Image> imgPub;

        CameraPublisher(ros::NodeHandle& nh, std::string const& camera_namespace, uint32_t const& buffer, bool const& latch = false)
                : infoPub(nh, image_transport::getCameraInfoTopic(camera_namespace), buffer, latch)
                , imgPub(nh, nh.resolveName(camera_namespace), buffer, latch) {}

    public:

        [[nodiscard]] uint32_t getNumSubscribers() const {
            return std::max(infoPub.getNumSubscribers(), imgPub.getNumSubscribers());
        }

        void publish(sensor_msgs::ImagePtr const& imgMsg, sensor_msgs::CameraInfoPtr const& infoMsg) {
            infoPub.publish(infoMsg);
            imgPub.publish(imgMsg);
        }
    };

    template <typename MessageType>
    class Subscriber {
        friend class MarathonTransport;
        ros::Subscriber marathon_sub;
        std::string name;
//        std::unique_ptr<Marathon::TableArchive<MessageType, ros::serialization::Serializer, false, false>> archive;

        Subscriber(ros::NodeHandle& nh, std::string const& name, uint32_t const& buffer, void(*cb)(MessageType))
                : marathon_sub(nh.subscribe<ros_marathon_example::MarathonMsg>(name + "/marathon", buffer, [=](const ros_marathon_example::MarathonMsgConstPtr & mmsg){
            Marathon::TableArchive<MessageType, ros::serialization::Serializer> archive(ros::file_log::getLogDirectory(), mmsg->archive_id, Marathon::ModeType::READ_ONLY);
            archive.seek(mmsg->row, Marathon::SeekReference::START);
            cb(archive.read());
            return;
        })) {}
    };

    class MarathonTransport {
        ros::NodeHandle& nh;
    public:
        explicit MarathonTransport(ros::NodeHandle& nh) : nh(nh) {}

        template <typename T>
        Publisher<T> advertise(std::string const& name, uint32_t const& buffer, bool const& latch = false) {
            return Publisher<T>(nh, name, buffer, latch);
        }

        template <typename T>
        Subscriber<T> subscribe(std::string const& name, uint32_t const& buffer, void(*cb)(T)) {
            return Subscriber<T>(nh, name, buffer, cb);
        }
    };
} // namespace ros_marathon

#endif //ros_marathon_example_ROS_MARATHON_HPP
